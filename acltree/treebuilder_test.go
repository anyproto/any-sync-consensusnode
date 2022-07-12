package acltree

//func createTreeFromThread(t thread.Thread, fromStart bool) (*Tree, error) {
//	treeBuilder := newTreeBuilder(t, keys.NewEd25519Decoder())
//	treeBuilder.Init()
//	return treeBuilder.Build(fromStart)
//}
//
//func TestACLTreeBuilder_UserJoinCorrectHeadsAndLen(t *testing.T) {
//	thread, err := threadbuilder.NewThreadBuilderWithTestName("threadbuilder/userjoinexample.yml")
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	res, err := createTreeFromThread(thread)
//	if err != nil {
//		t.Fatalf("build Tree should not result in an error: %v", res)
//	}
//
//	assert.equal(t, res.Heads(), []string{"C.1.1"})
//	assert.equal(t, res.Len(), 4)
//}
//
//func TestTreeBuilder_UserJoinTestTreeIterate(t *testing.T) {
//	thread, err := threadbuilder.NewThreadBuilderWithTestName("threadbuilder/userjoinexample.yml")
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	res, err := createTreeFromThread(thread)
//	if err != nil {
//		t.Fatalf("build Tree should not result in an error: %v", res)
//	}
//
//	assert.equal(t, res.Heads(), []string{"C.1.1"})
//	assert.equal(t, res.Len(), 4)
//	var changeIds []string
//	res.iterate(res.root, func(c *Change) (isContinue bool) {
//		changeIds = append(changeIds, c.Id)
//		return true
//	})
//	assert.equal(t, changeIds, []string{"A.1.1", "A.1.2", "B.1.1", "C.1.1"})
//}
//
//func TestTreeBuilder_UserRemoveTestTreeIterate(t *testing.T) {
//	thread, err := threadbuilder.NewThreadBuilderWithTestName("threadbuilder/userremoveexample.yml")
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	res, err := createTreeFromThread(thread)
//	if err != nil {
//		t.Fatalf("build Tree should not result in an error: %v", res)
//	}
//
//	assert.equal(t, res.Heads(), []string{"A.1.3"})
//	assert.equal(t, res.Len(), 4)
//	var changeIds []string
//	res.iterate(res.root, func(c *Change) (isContinue bool) {
//		changeIds = append(changeIds, c.Id)
//		return true
//	})
//	assert.equal(t, changeIds, []string{"A.1.1", "A.1.2", "B.1.1", "A.1.3"})
//}
