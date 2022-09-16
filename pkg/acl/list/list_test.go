package list

import (
	"github.com/anytypeio/go-anytype-infrastructure-experiments/pkg/acl/aclchanges/aclpb"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/pkg/acl/testutils/acllistbuilder"
	"github.com/anytypeio/go-anytype-infrastructure-experiments/util/keys/asymmetric/signingkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAclList_ACLState_UserInviteAndJoin(t *testing.T) {
	st, err := acllistbuilder.NewListStorageWithTestName("userjoinexample.yml")
	require.NoError(t, err, "building storage should not result in error")

	keychain := st.(*acllistbuilder.ACLListStorageBuilder).GetKeychain()

	aclList, err := BuildACLList(signingkey.NewEDPubKeyDecoder(), st)
	require.NoError(t, err, "building acl list should be without error")

	idA := keychain.GetIdentity("A")
	idB := keychain.GetIdentity("B")
	idC := keychain.GetIdentity("C")

	// checking final state
	assert.Equal(t, aclpb.ACLUserPermissions_Admin, aclList.ACLState().GetUserStates()[idA].Permissions)
	assert.Equal(t, aclpb.ACLUserPermissions_Writer, aclList.ACLState().GetUserStates()[idB].Permissions)
	assert.Equal(t, aclpb.ACLUserPermissions_Reader, aclList.ACLState().GetUserStates()[idC].Permissions)
	assert.Equal(t, aclList.Head().Content.CurrentReadKeyHash, aclList.ACLState().CurrentReadKeyHash())

	var records []*ACLRecord
	aclList.Iterate(func(record *ACLRecord) (IsContinue bool) {
		records = append(records, record)
		return true
	})

	// checking permissions at specific records
	assert.Equal(t, 3, len(records))

	_, err = aclList.ACLState().PermissionsAtRecord(records[1].Id, idB)
	assert.Error(t, err, "B should have no permissions at record 1")

	perm, err := aclList.ACLState().PermissionsAtRecord(records[2].Id, idB)
	assert.NoError(t, err, "should have no error with permissions of B in the record 2")
	assert.Equal(t, UserPermissionPair{
		Identity:   idB,
		Permission: aclpb.ACLUserPermissions_Writer,
	}, perm)
}

func TestAclList_ACLState_UserJoinAndRemove(t *testing.T) {
	st, err := acllistbuilder.NewListStorageWithTestName("userremoveexample.yml")
	require.NoError(t, err, "building storage should not result in error")

	keychain := st.(*acllistbuilder.ACLListStorageBuilder).GetKeychain()

	aclList, err := BuildACLList(signingkey.NewEDPubKeyDecoder(), st)
	require.NoError(t, err, "building acl list should be without error")

	idA := keychain.GetIdentity("A")
	idB := keychain.GetIdentity("B")
	idC := keychain.GetIdentity("C")

	// checking final state
	assert.Equal(t, aclpb.ACLUserPermissions_Admin, aclList.ACLState().GetUserStates()[idA].Permissions)
	assert.Equal(t, aclpb.ACLUserPermissions_Reader, aclList.ACLState().GetUserStates()[idC].Permissions)
	assert.Equal(t, aclList.Head().Content.CurrentReadKeyHash, aclList.ACLState().CurrentReadKeyHash())

	_, exists := aclList.ACLState().GetUserStates()[idB]
	assert.Equal(t, false, exists)

	var records []*ACLRecord
	aclList.Iterate(func(record *ACLRecord) (IsContinue bool) {
		records = append(records, record)
		return true
	})

	// checking permissions at specific records
	assert.Equal(t, 4, len(records))

	assert.NotEqual(t, records[2].Content.CurrentReadKeyHash, aclList.ACLState().CurrentReadKeyHash())

	perm, err := aclList.ACLState().PermissionsAtRecord(records[2].Id, idB)
	assert.NoError(t, err, "should have no error with permissions of B in the record 2")
	assert.Equal(t, UserPermissionPair{
		Identity:   idB,
		Permission: aclpb.ACLUserPermissions_Writer,
	}, perm)

	_, err = aclList.ACLState().PermissionsAtRecord(records[3].Id, idB)
	assert.Error(t, err, "B should have no permissions at record 3, because user should be removed")
}