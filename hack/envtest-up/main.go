//go:build smoke

// Command envtest-up starts a local kube-apiserver and etcd with envtest,
// installs the example's CRD, writes a kubeconfig for it, and runs until it is
// interrupted. It is for local smoke runs of the example controller without a
// cluster (hack/smoke-local-slot.sh). KUBEBUILDER_ASSETS must name the envtest
// binaries.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func main() {
	out := flag.String("out", "", "where to write the kubeconfig")
	crd := flag.String("crd", "examples/greeting-controller/crd.yaml", "the CRD to install")
	flag.Parse()

	env := &envtest.Environment{CRDDirectoryPaths: []string{*crd}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "envtest:", err)
		os.Exit(1)
	}
	defer func() { _ = env.Stop() }()

	kc := clientcmdapi.NewConfig()
	kc.Clusters["envtest"] = &clientcmdapi.Cluster{Server: cfg.Host, CertificateAuthorityData: cfg.CAData}
	kc.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData, Token: cfg.BearerToken}
	kc.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "envtest"}
	kc.CurrentContext = "envtest"
	if err := clientcmd.WriteToFile(*kc, *out); err != nil {
		fmt.Fprintln(os.Stderr, "kubeconfig:", err)
		os.Exit(1)
	}
	fmt.Println("ENVTEST-READY", cfg.Host)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
