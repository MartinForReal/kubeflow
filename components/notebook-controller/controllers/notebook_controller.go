/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	gogopb "github.com/gogo/protobuf/types"
	"github.com/kubeflow/kubeflow/components/notebook-controller/api/v1alpha1"
	"github.com/kubeflow/kubeflow/components/notebook-controller/pkg/culler"
	"github.com/kubeflow/kubeflow/components/notebook-controller/pkg/metrics"
	istionetworkingapiv1 "istio.io/api/networking/v1alpha3"
	istionetworkingv1 "istio.io/client-go/pkg/apis/networking/v1alpha3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"time"
)

const DefaultContainerPort = 8888
const DefaultServingPort = 80

// The default fsGroup of PodSecurityContext.
// https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#podsecuritycontext-v1-core
const DefaultFSGroup = int64(100)

/*
We generally want to ignore (not requeue) NotFound errors, since we'll get a
reconciliation request once the object exists, and requeuing in the meantime
won't help.
*/
func ignoreNotFound(err error) error {
	if apierrs.IsNotFound(err) {
		return nil
	}
	return err
}

// NotebookReconciler reconciles a Notebook object
type NotebookReconciler struct {
	client.Client
	Log           logr.Logger
	Scheme        *runtime.Scheme
	Metrics       *metrics.Metrics
	EventRecorder record.EventRecorder
}

// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kubeflow.org,resources=notebooks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kubeflow.org,resources=notebooks/status,verbs=get;update;patch

func (r *NotebookReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("notebook", req.NamespacedName)

	instance := &v1alpha1.Notebook{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		log.Error(err, "unable to fetch Notebook")
		return ctrl.Result{}, ignoreNotFound(err)
	}

	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name,
			Namespace: instance.Namespace,
		},
	}

	result, err := CreateOrUpdate(ctx, r.Client, ss, func() error {
		if ss.CreationTimestamp.IsZero() {
			log.Info("Creating StatefulSet", "namespace", ss.Namespace, "name", ss.Name)
		} else {
			log.Info("Updating StatefulSet", "namespace", ss.Namespace, "name", ss.Name)
		}
		controllerutil.SetControllerReference(instance, ss, r.Scheme)
		return generateStatefulSet(ss, instance)
	}, func(origin runtime.Object, new runtime.Object) bool {
		originSts := origin.(*appsv1.StatefulSet)
		newSts := new.(*appsv1.StatefulSet)
		return equality.Semantic.DeepDerivative(originSts.Spec, newSts.Spec)
	})

	if err != nil {
		log.Error(err, "Failed to ensure statefulset")
		return ctrl.Result{}, err
	}
	if result == controllerutil.OperationResultCreated {
		r.Metrics.NotebookCreation.WithLabelValues(ss.Namespace).Inc()
	}

	// Reconcile service
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name,
			Namespace: instance.Namespace,
		},
	}
	_, err = CreateOrUpdate(ctx, r.Client, service, func() error {
		if service.CreationTimestamp.IsZero() {
			log.Info("Creating service", "namespace", service.Namespace, "name", service.Name)
		} else {
			log.Info("Updating service", "namespace", service.Namespace, "name", service.Name)
		}
		controllerutil.SetControllerReference(instance, service, r.Scheme)
		return generateService(service, instance)
	}, func(origin runtime.Object, new runtime.Object) bool {
		originSts := origin.(*corev1.Service)
		newSts := new.(*corev1.Service)
		return equality.Semantic.DeepDerivative(originSts.Spec, newSts.Spec)
	})
	if err != nil {
		log.Error(err, "Failed to ensure service")
	}

	// Reconcile virtual service if we use ISTIO.
	if os.Getenv("USE_ISTIO") == "true" {
		// Reconcile service
		virtualService := &istionetworkingv1.VirtualService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      instance.Name,
				Namespace: instance.Namespace,
			},
		}
		_, err = CreateOrUpdate(ctx, r.Client, virtualService, func() error {
			if service.CreationTimestamp.IsZero() {
				log.Info("Creating virtual service", "namespace", virtualService.Namespace, "name", virtualService.Name)
			} else {
				log.Info("Updating virtual service", "namespace", virtualService.Namespace, "name", virtualService.Name)
			}
			controllerutil.SetControllerReference(instance, virtualService, r.Scheme)
			return generateVirtualService(virtualService, instance)
		}, func(origin runtime.Object, new runtime.Object) bool {
			originSts := origin.(*istionetworkingv1.VirtualService)
			newSts := new.(*istionetworkingv1.VirtualService)
			return equality.Semantic.DeepDerivative(originSts.Spec, newSts.Spec)
		})
		if err != nil {
			log.Error(err, "Failed to ensure virtualservice")
		}
	}
	instance.Status.ReadyReplicas = ss.Status.ReadyReplicas
	if ss.Status.ReadyReplicas == 1 {
		// Check the pod status
		pod := &corev1.Pod{}
		podFound := false
		err = r.Get(ctx, types.NamespacedName{Name: ss.Name + "-0", Namespace: ss.Namespace}, pod)
		if err != nil && apierrs.IsNotFound(err) {
			// This should be reconciled by the StatefulSet
			log.Info("Pod not found...")
		} else if err != nil {
			log.Error(err, "failed to get pod")
			return ctrl.Result{}, err
		} else {
			// Got the pod
			podFound = true
			containerIndex := 0
			for i, value := range pod.Status.ContainerStatuses {
				if value.Name == instance.Name {
					containerIndex = i
				}
			}
			if len(pod.Status.ContainerStatuses) > containerIndex {
				if !equality.Semantic.DeepDerivative(pod.Status.ContainerStatuses[containerIndex].State, instance.Status.ContainerState) {
					log.Info("Updating container state: ", "namespace", instance.Namespace, "name", instance.Name)
					cs := pod.Status.ContainerStatuses[containerIndex].State
					instance.Status.ContainerState = cs
					oldConditions := instance.Status.Conditions
					newCondition := getNextCondition(cs)
					// Append new condition
					if len(oldConditions) == 0 || oldConditions[0].Type != newCondition.Type ||
						oldConditions[0].Reason != newCondition.Reason ||
						oldConditions[0].Message != newCondition.Message {
						log.Info("Appending to conditions: ", "namespace", instance.Namespace, "name", instance.Name, "type", newCondition.Type, "reason", newCondition.Reason, "message", newCondition.Message)
						instance.Status.Conditions = append([]v1alpha1.NotebookCondition{newCondition}, oldConditions...)
					}
					if err := r.Client.Status().Update(ctx, instance); err != nil {
						log.Error(err, "failed to update status")
						return ctrl.Result{}, err
					}
				}
			}
		}

		// Check if the Notebook needs to be stopped
		if podFound && culler.NotebookNeedsCulling(instance.ObjectMeta) {
			log.Info(fmt.Sprintf(
				"Notebook %s/%s needs culling. Setting annotations",
				instance.Namespace, instance.Name))

			// Set annotations to the Notebook
			culler.SetStopAnnotation(&instance.ObjectMeta, r.Metrics)
			r.Metrics.NotebookCullingCount.WithLabelValues(instance.Namespace, instance.Name).Inc()
			err = r.Update(ctx, instance)
			if err != nil {
				return ctrl.Result{}, err
			}
		} else if podFound && !culler.StopAnnotationIsSet(instance.ObjectMeta) {
			// The Pod is either too fresh, or the idle time has passed and it has
			// received traffic. In this case we will be periodically checking if
			// it needs culling.
			return ctrl.Result{RequeueAfter: culler.GetRequeueTime()}, nil
		}
	}
	return ctrl.Result{}, nil
}

func getNextCondition(cs corev1.ContainerState) v1alpha1.NotebookCondition {
	var nbtype = ""
	var nbreason = ""
	var nbmsg = ""

	if cs.Running != nil {
		nbtype = "Running"
	} else if cs.Waiting != nil {
		nbtype = "Waiting"
		nbreason = cs.Waiting.Reason
		nbmsg = cs.Waiting.Message
	} else {
		nbtype = "Terminated"
		nbreason = cs.Terminated.Reason
		nbmsg = cs.Terminated.Reason
	}

	newCondition := v1alpha1.NotebookCondition{
		Type:          nbtype,
		LastProbeTime: metav1.Now(),
		Reason:        nbreason,
		Message:       nbmsg,
	}
	return newCondition
}

func generateStatefulSet(ss *appsv1.StatefulSet, instance *v1alpha1.Notebook) error {
	if ss == nil || instance == nil {
		return errors.New("statefulset or instance is nil")
	}
	replicas := int32(1)
	if culler.StopAnnotationIsSet(instance.ObjectMeta) {
		replicas = 0
	}

	ss.Spec.Replicas = &replicas
	ss.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: map[string]string{
			"statefulset": instance.Name,
		},
	}
	ss.Spec.Template = corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			"statefulset":   instance.Name,
			"notebook-name": instance.Name,
		}},
		Spec: instance.Spec.Template.Spec,
	}
	// copy all of the Notebook labels to the pod including poddefault related labels
	l := &ss.Spec.Template.ObjectMeta.Labels
	for k, v := range instance.ObjectMeta.Labels {
		(*l)[k] = v
	}

	podSpec := &ss.Spec.Template.Spec
	container := &podSpec.Containers[0]
	if container.WorkingDir == "" {
		container.WorkingDir = "/home/jovyan"
	}
	if container.Ports == nil {
		container.Ports = []corev1.ContainerPort{
			{
				ContainerPort: DefaultContainerPort,
				Name:          "notebook-port",
				Protocol:      "TCP",
			},
		}
	}
	if len(container.Ports) == 1 {
		container.LivenessProbe = &corev1.Probe{
			Handler: corev1.Handler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/notebook/" + instance.Namespace + "/" + instance.Name + "/api/status",
					Port:   intstr.FromInt(int(container.Ports[0].ContainerPort)),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 20,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		}
		container.ReadinessProbe = &corev1.Probe{
			Handler: corev1.Handler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/notebook/" + instance.Namespace + "/" + instance.Name + "/api/status",
					Port:   intstr.FromInt(int(container.Ports[0].ContainerPort)),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 20,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		}
	}
	container.Env = append(container.Env, corev1.EnvVar{
		Name:  "NB_PREFIX",
		Value: "/notebook/" + instance.Namespace + "/" + instance.Name,
	})

	// For some platforms (like OpenShift), adding fsGroup: 100 is troublesome.
	// This allows for those platforms to bypass the automatic addition of the fsGroup
	// and will allow for the Pod Security Policy controller to make an appropriate choice
	// https://github.com/kubernetes-sigs/controller-runtime/issues/4617
	if value, exists := os.LookupEnv("ADD_FSGROUP"); !exists || value == "true" {
		if podSpec.SecurityContext == nil {
			fsGroup := DefaultFSGroup
			podSpec.SecurityContext = &corev1.PodSecurityContext{
				FSGroup: &fsGroup,
			}
		}
	}
	return nil
}

func generateService(service *corev1.Service, instance *v1alpha1.Notebook) error {
	if service == nil || instance == nil {
		return errors.New("service or instance is nil")
	}
	// Define the desired Service object
	port := DefaultContainerPort
	containerPorts := instance.Spec.Template.Spec.Containers[0].Ports
	if containerPorts != nil {
		port = int(containerPorts[0].ContainerPort)
	}
	service.Spec.Type = "ClusterIP"
	service.Spec.Selector = map[string]string{"statefulset": instance.Name}
	service.Spec.Ports = []corev1.ServicePort{
		{
			// Make port name follow Istio pattern so it can be managed by istio rbac
			Name:       "http-" + instance.Name,
			Port:       DefaultServingPort,
			TargetPort: intstr.FromInt(port),
			Protocol:   "TCP",
		},
	}
	return nil
}

func virtualServiceName(kfName string, namespace string) string {
	return fmt.Sprintf("notebook-%s-%s", namespace, kfName)
}

func generateVirtualService(virtualService *istionetworkingv1.VirtualService, instance *v1alpha1.Notebook) error {
	if virtualService == nil || instance == nil {
		return errors.New("virtual service or instance is nil")
	}
	namespace := instance.Namespace
	name := instance.Name
	clusterDomain := "cluster.local"
	prefix := fmt.Sprintf("/notebook/%s/%s/", namespace, name)
	rewrite := fmt.Sprintf("/notebook/%s/%s/", namespace, name)
	if clusterDomainFromEnv, ok := os.LookupEnv("CLUSTER_DOMAIN"); ok {
		clusterDomain = clusterDomainFromEnv
	}
	service := fmt.Sprintf("%s.%s.svc.%s", name, namespace, clusterDomain)

	virtualService.Spec.Hosts = []string{"*"}

	istioGateway := os.Getenv("ISTIO_GATEWAY")
	if len(istioGateway) == 0 {
		istioGateway = "kubeflow/kubeflow-gateway"
	}
	virtualService.Spec.Gateways = []string{istioGateway}

	virtualService.Spec.Http = []*istionetworkingapiv1.HTTPRoute{
		{

			Match: []*istionetworkingapiv1.HTTPMatchRequest{
				{
					Uri: &istionetworkingapiv1.StringMatch{
						MatchType: &istionetworkingapiv1.StringMatch_Prefix{
							Prefix: prefix,
						},
					},
				},
			},
			Rewrite: &istionetworkingapiv1.HTTPRewrite{
				Uri: rewrite,
			},
			Route: []*istionetworkingapiv1.HTTPRouteDestination{
				{
					Destination: &istionetworkingapiv1.Destination{
						Host: service,
						Port: &istionetworkingapiv1.PortSelector{
							Number: DefaultServingPort,
						},
					},
				},
			},
			Timeout: gogopb.DurationProto(time.Second * 300),
		},
	}

	return nil
}

func nbNameFromInvolvedObject(c client.Client, object *corev1.ObjectReference) (string, error) {
	name, namespace := object.Name, object.Namespace

	if object.Kind == "StatefulSet" {
		return name, nil
	}
	if object.Kind == "Pod" {
		pod := &corev1.Pod{}
		err := c.Get(
			context.TODO(),
			types.NamespacedName{
				Namespace: namespace,
				Name:      name,
			},
			pod,
		)
		if err != nil {
			return "", err
		}
		if nbName, ok := pod.Labels["notebook-name"]; ok {
			return nbName, nil
		}
	}
	return "", fmt.Errorf("object isn't related to a Notebook")
}

func (r *NotebookReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Notebook{}).
		Owns(&istionetworkingv1.VirtualService{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.StatefulSet{}).Complete(r)
}

type MutateFn func() error
type CompareFn func(origin runtime.Object, new runtime.Object) bool

func CreateOrUpdate(ctx context.Context, c client.Client, obj runtime.Object, f MutateFn, compare CompareFn) (controllerutil.OperationResult, error) {
	key, err := client.ObjectKeyFromObject(obj)
	if err != nil {
		return controllerutil.OperationResultNone, err
	}

	if err := c.Get(ctx, key, obj); err != nil {
		if !apierrs.IsNotFound(err) {
			return controllerutil.OperationResultNone, err
		}
		if err := mutate(f, key, obj); err != nil {
			return controllerutil.OperationResultNone, err
		}
		if err := c.Create(ctx, obj); err != nil {
			return controllerutil.OperationResultNone, err
		}
		return controllerutil.OperationResultCreated, nil
	}

	existing := obj.DeepCopyObject()
	if err := mutate(f, key, obj); err != nil {
		return controllerutil.OperationResultNone, err
	}

	if compare(existing, obj) {
		return controllerutil.OperationResultNone, nil
	}

	if err := c.Update(ctx, obj); err != nil {
		return controllerutil.OperationResultNone, err
	}
	return controllerutil.OperationResultUpdated, nil
}

// mutate wraps a MutateFn and applies validation to its result
func mutate(f MutateFn, key client.ObjectKey, obj runtime.Object) error {
	if err := f(); err != nil {
		return err
	}
	if newKey, err := client.ObjectKeyFromObject(obj); err != nil || key != newKey {
		return fmt.Errorf("MutateFn cannot mutate object name and/or object namespace")
	}
	return nil
}
