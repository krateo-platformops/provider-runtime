package reconciler

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	clientevents "k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
)

// configManager is a manager with a config and nothing else.
type configManager struct {
	manager.Manager
	cfg *rest.Config
}

func (m configManager) GetConfig() *rest.Config        { return m.cfg }
func (m configManager) GetRESTMapper() meta.RESTMapper { return meta.NewDefaultRESTMapper(nil) }
func (m configManager) GetEventRecorder(string) recorder.EventRecorder {
	return &clientevents.FakeRecorder{}
}

// The default Shared has every client, Discovery too: without it what a kind
// that is gone held stays (credentialuse design §6.4), and nothing else says
// so.
func TestTheDefaultSharedHasEveryClient(t *testing.T) {
	s, err := newDefaultShared(configManager{cfg: &rest.Config{Host: "https://localhost"}})
	if err != nil {
		t.Fatal(err)
	}
	// What the Shared has, whose field isn't exported: every client of it, one
	// added later too.
	var found bool
	v := reflect.ValueOf(s).Elem()
	for i := range v.NumField() {
		if v.Field(i).Type() != reflect.TypeFor[credentialuse.Clients]() {
			continue
		}
		found = true
		c := v.Field(i)
		for j := range c.NumField() {
			if c.Field(j).IsZero() {
				t.Errorf("the default Shared's Clients.%s isn't set", c.Type().Field(j).Name)
			}
		}
	}
	if !found {
		t.Error("the Shared has no field of type Clients")
	}
}

// A manager's Discovery reads the API server's documents.
func TestTheDefaultSharedReadsDiscovery(t *testing.T) {
	requireTraceAPIServer(t)
	m, err := manager.New(traceEnv.Config, manager.Options{Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	c, err := credentialUseClients(m)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	data, err := c.Discovery.Get().AbsPath("/apis", widgetGVK.Group, widgetGVK.Version).SetHeader("Accept", "application/json").Do(ctx).Raw()
	if err != nil {
		t.Fatal(err)
	}
	var list metav1.APIResourceList
	if err := json.Unmarshal(data, &list); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range list.APIResources {
		found = found || r.Name == "widgets" && r.Kind == "Widget"
	}
	if !found {
		t.Errorf("the discovery of %s has no Widget: %s", widgetGVK.GroupVersion(), data)
	}
}
