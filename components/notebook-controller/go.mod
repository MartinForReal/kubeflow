module github.com/kubeflow/kubeflow/components/notebook-controller

go 1.12

require (
	github.com/go-logr/logr v0.1.0
	github.com/gogo/protobuf v1.3.1
	github.com/prometheus/client_golang v1.0.0
	google.golang.org/protobuf v1.25.0 // indirect
	istio.io/api v0.0.0-20200707142133-4eaf05f2696c
	istio.io/client-go v0.0.0-20200707144405-c16d7aa4ac16
	k8s.io/api v0.18.2
	k8s.io/apimachinery v0.18.2
	k8s.io/client-go v0.18.2
	sigs.k8s.io/controller-runtime v0.6.0
)

replace github.com/kubeflow/kubeflow/components/common => ../common
