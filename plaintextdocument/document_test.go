package plaintextdocument

//
//import (
//	"github.com/anytypeio/go-anytype-infrastructure-experiments/testutils/threadbuilder"
//	"github.com/stretchr/testify/assert"
//	"testing"
//)
//
//func TestDocument_Build(t *testing.T) {
//	thread, err := threadbuilder.NewThreadBuilderWithTestName("threadbuilder/userjoinexample.yml")
//	if err != nil {
//		t.Fatal(err)
//	}
//	keychain := thread.GetKeychain()
//	accountData := &AccountData{
//		Identity: keychain.GetIdentity("A"),
//		EncKey:   keychain.EncryptionKeys["A"],
//	}
//	doc := NewDocument(thread, NewPlainTextDocumentStateProvider(), accountData)
//	res, err := doc.Build()
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	st := res.(*DocumentState)
//	assert.Equal(t, st.Text, "some text|first")
//}
//
//func TestDocument_Update(t *testing.T) {
//	thread, err := threadbuilder.NewThreadBuilderWithTestName("threadbuilder/userjoinexample.yml")
//	if err != nil {
//		t.Fatal(err)
//	}
//	keychain := thread.GetKeychain()
//	accountData := &AccountData{
//		Identity: keychain.GetIdentity("A"),
//		EncKey:   keychain.EncryptionKeys["A"],
//	}
//	doc := NewDocument(thread, NewPlainTextDocumentStateProvider(), accountData)
//	res, err := doc.Build()
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	st := res.(*DocumentState)
//	assert.Equal(t, st.Text, "some text|first")
//
//	rawChs := thread.GetUpdatedChanges()
//	res, updateResult, err := doc.Update(rawChs...)
//	assert.Equal(t, updateResult, UpdateResultAppend)
//	assert.Equal(t, res.(*DocumentState).Text, "some text|first|second")
//}
