package drive

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"upspin.io/cloud/storage"
	"upspin.io/log"
)

var (
	client      storage.Storage
	testDataStr = fmt.Sprintf("This is test at %v", time.Now())
	testData    = []byte(testDataStr)
	fileName    = fmt.Sprintf("test-file-%d", time.Now().Second())

	accessToken  = flag.String("access-token", "", "bucket name to use for testing")
	refreshToken = flag.String("refresh-token", "", "region to use for the test bucket")
	expiry       = flag.String("expiry", "2017-10-12T09:45:38+02:00", "RFC3999 format time stamp")
	runE2E       = flag.Bool("run-e2e", false, "enable to run tests against an actual Drive account")
)

// This is more of a regression test as it uses the running cloud
// storage in prod. However, since S3 is always available, we accept
// relying on it.
func TestPutAndDownload(t *testing.T) {
	err := client.Put(fileName, testData)
	if err != nil {
		t.Fatalf("Can't put: %v", err)
	}
	data, err := client.Download(fileName)
	if err != nil {
		t.Fatalf("Can't Download: %v", err)
	}
	if string(data) != testDataStr {
		t.Errorf("Expected %q got %q", testDataStr, string(data))
	}
}

func TestDelete(t *testing.T) {
	err := client.Put(fileName, testData)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Delete(fileName)
	if err != nil {
		t.Fatalf("Expected no errors, got %v", err)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !*runE2E {
		log.Printf(`

cloud/storage/drive: skipping test as it requires Drive access. To enable this
test, set the -run-e2e flag along with valid -access-token and -refresh-token
flag values.

`)
		os.Exit(0)
	}
	if *accessToken == "" || *refreshToken == "" {
		log.Printf(`

cloud/storage/drive: to run the e2e tests, please supply the additional -access-token
and -refresh-token flags for OAuth authentication. Skipping for now...

`)
		os.Exit(0)
	}

	// Create client that writes to test bucket.
	var err error
	client, err = storage.Dial("drive",
		storage.WithKeyValue("accessToken", *accessToken),
		storage.WithKeyValue("refreshToken", *refreshToken),
		storage.WithKeyValue("tokenType", "Bearer"),
		storage.WithKeyValue("expiry", *expiry))
	if err != nil {
		log.Fatalf("cloud/storage/drive: couldn't set up client: %v", err)
	}

	code := m.Run()

	// Clean up.
	if err := client.(*driveImpl).cleanup(); err != nil {
		log.Printf("cloud/storage/drive: clean-up failed: %v", err)
	}

	os.Exit(code)
}

func (d *driveImpl) cleanup() error {
	q := "name contains 'test-file-'"
	call := d.files.List().Spaces("appDataFolder").Q(q).Fields("files(id, name)")
	r, err := call.Do()
	if err != nil {
		return err
	}
	var er error
	for _, f := range r.Files {
		if err := d.files.Delete(f.Id).Do(); err != nil {
			er = err
		}
		d.cache.Remove(f.Name)
	}
	return er
}
