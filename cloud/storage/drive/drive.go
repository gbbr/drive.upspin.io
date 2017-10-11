package drive // import "drive.upspin.io/cloud/storage/drive"

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"drive.upspin.io/config"
	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"upspin.io/cache"
	"upspin.io/cloud/storage"
	"upspin.io/errors"
	"upspin.io/upspin"
)

func init() {
	storage.Register("drive", New)
}

// LRUSize holds the maximum number of entries that should live in the LRU cache.
// Since it only maps file names to file IDs, 500 should be affordable to any server.
//
// TODO(gbbr): Make this optionally configurable via command line.
const LRUSize = 500

// ErrTokenOpts is returned when options are missing from the storage configuration
var ErrTokenOpts = errors.Errorf("one or more required options are missing, need: accessToken, tokenType, refreshToken, expiry")

// New initializes a new Storage which stores data to Google Drive.
func New(o *storage.Opts) (storage.Storage, error) {
	const op = "cloud/storage/drive.New"
	var a, t, r, e string
	ok := true
	a, ok = o.Opts["accessToken"]
	t, ok = o.Opts["tokenType"]
	r, ok = o.Opts["refreshToken"]
	e, ok = o.Opts["expiry"]
	if !ok {
		return nil, errors.E(op, errors.Internal, ErrTokenOpts)
	}
	et, err := time.Parse(time.RFC3339, e)
	if err != nil {
		return nil, errors.E(op, errors.Internal, errors.Errorf("couldn't parse expiry: ", err))
	}
	ctx := context.Background()
	client := config.OAuth2.Client(ctx, &oauth2.Token{
		AccessToken:  a,
		TokenType:    t,
		RefreshToken: r,
		Expiry:       et,
	})
	svc, err := drive.New(client)
	if err != nil {
		return nil, errors.E(op, errors.Internal, errors.Errorf("unable to retreieve drive client: %v", err))
	}
	return &driveImpl{
		files: svc.Files,
		cache: cache.NewLRU(LRUSize),
	}, nil
}

var _ storage.Storage = (*driveImpl)(nil)

// driveImpl is an implementation of Storage that connects to a Google Drive backend.
type driveImpl struct {
	// files holds the FilesService used to interact with the Drive API.
	files *drive.FilesService
	// cache will map file names to file IDs to avoid hitting the HTTP API
	// twice on each download.
	cache *cache.LRU
}

func (d *driveImpl) LinkBase() (string, error) {
	// Drive does have a LinkBase but it expects it to be followed by the file ID,
	// not by the name of the file. Since we can not use the 'ref' as the file ID
	// this service is not available.
	return "", upspin.ErrNotSupported
}

func (d *driveImpl) Download(ref string) ([]byte, error) {
	const op = "cloud/storage/drive.Download"
	id, err := d.fileId(ref)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.E(op, errors.NotExist, err)
		}
		return nil, errors.E(op, errors.IO, err)
	}
	resp, err := d.files.Get(id).Download()
	if err != nil {
		return nil, errors.E(op, errors.IO, err)
	}
	defer resp.Body.Close()
	slurp, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.E(op, errors.IO, err)
	}
	return slurp, nil
}

func (d *driveImpl) Put(ref string, contents []byte) error {
	const op = "cloud/storage/drive.Put"
	// check if file already exists
	id, err := d.fileId(ref)
	if err != nil && !os.IsNotExist(err) {
		return errors.E(op, errors.IO, err)
	}
	if id != "" {
		// if it does, delete it to ensure uniquness because Google Drive allows
		// multiple files with the same name to coexist in the same folder
		if err := d.Delete(id); err != nil {
			return err
		}
	}
	call := d.files.Create(&drive.File{
		Name:    ref,
		Parents: []string{"appDataFolder"},
	})
	contentType := googleapi.ContentType("application/octet-stream")
	_, err = call.Media(bytes.NewReader(contents), contentType).Do()
	if err != nil {
		return errors.E(op, errors.IO, err)
	}
	return nil
}

func (d *driveImpl) Delete(ref string) error {
	const op = "cloud/storage/drive.Download"
	id, err := d.fileId(ref)
	if err != nil {
		if os.IsNotExist(err) {
			// nothing to delete
			return nil
		}
		return errors.E(op, errors.IO, err)
	}
	if err := d.files.Delete(id).Do(); err != nil {
		return errors.E(op, errors.IO, err)
	}
	d.cache.Remove(ref)
	return nil
}

// fileId returns the file ID of the first file found under the given name.
func (d *driveImpl) fileId(name string) (string, error) {
	// try cache first
	if id, ok := d.cache.Get(name); ok {
		return id.(string), nil
	}
	q := fmt.Sprintf("name='%s'", name)
	call := d.files.List().Spaces("appDataFolder").Q(q).Fields("files(id)")
	r, err := call.Do()
	if err != nil {
		return "", err
	}
	if len(r.Files) == 0 {
		return "", os.ErrNotExist
	}
	// In Drive it is possible that multiple files share the same name under distinct
	// IDs. It is the responsibility of the Storage user to assure that this collision
	// doesn't happen by using unique ref names. The default implementation uses SHA256
	// hashes of the content which ensure uniqueness.
	id := r.Files[0].Id
	d.cache.Add(name, id)
	return id, nil
}
