#include <assert.h>
#include <string.h> //Used for memmove
#include <iostream>
#include <queue>
#include <set>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+3;//changed from +2 to +3 because we are allocating a leaf node
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+3;//changed from +2 to +3 because we are allocating a leaf node
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);
    SIZE_T ptr;
    rc=CreateLeafNode(ptr,superblock_index+1);
    if (rc) { return rc; }
    // Append the newleaf to the root's first pointer
    newrootnode.SetPtr(0,ptr);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }
    // changed from +2 to +3 because we are allocating a leaf node
    for (SIZE_T i=superblock_index+3; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
	if (op==BTREE_OP_LOOKUP) { 
	  return b.GetVal(offset,value);
	} else { 
	  // BTREE_OP_UPDATE
	  // WRITE ME
	  return ERROR_UNIMPL;
	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

// The function to create a new leaf node in one disk write.
ERROR_T BTreeIndex::CreateLeafNode(SIZE_T &ptr, SIZE_T rootnode){
  
  ERROR_T rc;
  // Allocate a new node. NOTE that AllocateNode cannot take const SIZE_T as an argument.
  ptr=superblock_index+2;

  if (ptr==0) { 
    return ERROR_NOSPACE;
  }
  BTreeNode node(BTREE_LEAF_NODE,
        superblock.info.keysize,
        superblock.info.valuesize,
        buffercache->GetBlockSize());
  node.info.rootnode=rootnode;
  //node.info.freelist=superblock_index+2;//Don't know what free list does.

  buffercache->NotifyAllocateBlock(ptr);

  rc=node.Serialize(buffercache,ptr);
  if (rc) { return rc; }
  return ERROR_NOERROR;
}

// The function to create a new leaf node in one disk write. Input includes the intial key and value pair
ERROR_T BTreeIndex::CreateLeafNode(SIZE_T &ptr, SIZE_T rootnode, const KEY_T &key, const VALUE_T &value){
  
  ERROR_T rc;
  // Allocate a new node. NOTE that AllocateNode cannot take const SIZE_T as an argument.
  AllocateNode(ptr);
  //ptr=superblock.info.freelist;

  if (ptr==0) { 
    return ERROR_NOSPACE;
  }
  BTreeNode node(BTREE_LEAF_NODE,
        superblock.info.keysize,
        superblock.info.valuesize,
        buffercache->GetBlockSize());
  node.info.rootnode=rootnode;
  //node.info.freelist=superblock_index+2;//Don't know what free list does.
  node.info.numkeys=1;
  node.SetKey(0,key);
  node.SetVal(0,value);

  buffercache->NotifyAllocateBlock(ptr);

  rc=node.Serialize(buffercache,ptr);
  if (rc) { return rc; }
  return ERROR_NOERROR;
}

// The function to create a new leaf node in one disk write. 
// Input includes the memory address for memmove and how many pairs
ERROR_T BTreeIndex::CreateLeafNode(SIZE_T &ptr, SIZE_T rootnode, char * memAddress, SIZE_T numPairsToCopy ){
  
  ERROR_T rc;
  // Allocate a new node. NOTE that AllocateNode cannot take const SIZE_T as an argument.
  AllocateNode(ptr);
  //ptr=superblock.info.freelist;

  if (ptr==0) { 
    return ERROR_NOSPACE;
  }
  BTreeNode node(BTREE_LEAF_NODE,
        superblock.info.keysize,
        superblock.info.valuesize,
        buffercache->GetBlockSize());
  node.info.rootnode=rootnode;
  //node.info.freelist=superblock_index+2;//Don't know what free list does.
  node.info.numkeys=numPairsToCopy;
  memmove(node.ResolveKey(0),memAddress,(node.info.keysize+node.info.valuesize)*numPairsToCopy);
            
  buffercache->NotifyAllocateBlock(ptr);

  rc=node.Serialize(buffercache,ptr);
  if (rc) { return rc; }
  return ERROR_NOERROR;
}

// The function to create a new interior node in one disk write. 
// Input includes the memory address for memmove and how many pairs
// Note, when creating an interior node, we copy from address of a pointer, and we copy numPairsToCopy+1 ptrs and numPairsToCopy keys.
ERROR_T BTreeIndex::CreateInteriorNode(SIZE_T &ptr, SIZE_T rootnode, char * memAddress, SIZE_T numPairsToCopy ){
  
  ERROR_T rc;
  // Allocate a new node. NOTE that AllocateNode cannot take const SIZE_T as an argument.
  AllocateNode(ptr);
  //ptr=superblock.info.freelist;

  if (ptr==0) { 
    return ERROR_NOSPACE;
  }
  BTreeNode node(BTREE_INTERIOR_NODE,
        superblock.info.keysize,
        superblock.info.valuesize,
        buffercache->GetBlockSize());
  node.info.rootnode=rootnode;
  //node.info.freelist=superblock_index+2;//Don't know what free list does.
  node.info.numkeys=numPairsToCopy;
  // Things we are copying: ptr, key, ptr, key... ptr, key, ptr.
  memmove(node.ResolvePtr(0),memAddress,(sizeof(SIZE_T)+node.info.keysize)*numPairsToCopy+sizeof(SIZE_T));
            
  buffercache->NotifyAllocateBlock(ptr);

  rc=node.Serialize(buffercache,ptr);
  if (rc) { return rc; }
  return ERROR_NOERROR;
}

// The function to create a new root node and append the old rootnode as the new rootnode's first pointer in 3 reads and 3 disk write
// Input includes the memory address for memmove and how many pairs
ERROR_T BTreeIndex::CreateNewRootNode(SIZE_T &ptr, const SIZE_T &oldrootnode){
  
  ERROR_T rc;
  // Allocate a new node. NOTE that AllocateNode cannot take const SIZE_T as an argument.
  AllocateNode(ptr);
  //ptr=superblock.info.freelist;

  if (ptr==0) { 
    return ERROR_NOSPACE;
  }
  BTreeNode node(BTREE_ROOT_NODE,
        superblock.info.keysize,
        superblock.info.valuesize,
        buffercache->GetBlockSize());
  node.info.rootnode=superblock_index;
  //node.info.freelist=superblock_index+2;//Don't know what free list does.
  node.info.numkeys=0;
  node.SetPtr(0,oldrootnode);
  buffercache->NotifyAllocateBlock(ptr);

  rc=node.Serialize(buffercache,ptr);
  if (rc) { return rc; }
  //Writes the new rootnode's address into superblock's first pointer
  // rc=superblock.Unserialize(buffercache,superblock_index);
  // if (rc) { return rc; }
  superblock.info.rootnode=ptr;
  rc=superblock.Serialize(buffercache,superblock_index);
  if (rc) { return rc; }
  //Change the old root node's type to interior node.
  BTreeNode oldroot;
  rc=oldroot.Unserialize(buffercache,oldrootnode);
  if (rc) { return rc; }
  oldroot.info.nodetype=BTREE_INTERIOR_NODE;
  oldroot.info.rootnode=ptr;
  rc=oldroot.Serialize(buffercache,oldrootnode);
  if (rc) { return rc; }
  return ERROR_NOERROR;
}


// The function to split an internal node. The input is the node, the key and the pointer to be inserted after/during the split
ERROR_T BTreeIndex::SplitInternal(const SIZE_T &node, const KEY_T &key, const SIZE_T &inputPtr){
  // Splitting an internal node:
  // The difference is xl contains [m-2]-1 smallest keys and xr contains [m/2] largest keys. 
  // Note that the [m/2]th key J is not placed in xl or xr, but is used to be a key in parent node
  // Make J the parent of xl and xr, and push j together with its child pointers(to xl) into the parent of x. 
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  SIZE_T offset_ptr;
  KEY_T testkey;
  KeyValuePair testkeyvalue;
  SIZE_T ptr;
  SIZE_T testptr;
  SIZE_T xl_ptr;
  SIZE_T xr_ptr;
  BTreeNode xl_node;
  BTreeNode xr_node;
  BTreeNode parentNode;
  KEY_T newkey;//The key used in parent node
  SIZE_T xl_ptrInParent;
  SIZE_T xr_ptrInParent;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }
  //If the block is the superblock, then that means we need to add another layer to the b-tree. 
  // i.e. add 1 to the height of b-tree
  if (b.info.nodetype==BTREE_ROOT_NODE){
    SIZE_T newrootnode;
    CreateNewRootNode(newrootnode, node);
    rc= b.Unserialize(buffercache,node);
    if (rc!=ERROR_NOERROR) { 
      return rc;
    }
  }

  //check whether the node is internal node
  assert(b.info.nodetype==BTREE_INTERIOR_NODE);
  //check that we indeed have no free space.
  assert(b.info.GetNumSlotsAsInterior()<=b.info.numkeys);
  offset_ptr=0;
  // Find the position to insert key and value.
  for (offset=0;offset<b.info.numkeys;offset++) { 
    rc=b.GetKey(offset,testkey);
    if (rc) {  return rc; }
    if (key<testkey || key==testkey) {
      // OK, so we now have the first key that's larger
      // So we record the offset and quit
      offset_ptr=offset+1;
      break;
    }
  }
  if (offset==b.info.numkeys){
    offset_ptr=offset;
  }
  if (offset_ptr<(b.info.numkeys+1)/2+1){
    // If the key to be inserted is in xl

    // Allocate right node
    rc=CreateInteriorNode(xr_ptr, b.info.rootnode, b.ResolvePtr(b.info.numkeys/2), b.info.numkeys/2);
    b.GetKey((b.info.numkeys)/2-1,newkey);//Get the new key for right side
    //b.GetPtr(b.info.numkeys/2,xr_ptrInParent);//Get the new ptr for right side
    if (rc) { return rc; }
    // Do the left node. Use the original node as the left node
    xl_ptr=node;
    // Move the memory after the insertion place one forward.
    // void * memmove ( void * destination, const void * source, size_t num );
    memmove(b.ResolveKey(offset_ptr),b.ResolveKey(offset_ptr-1),sizeof(SIZE_T)+(b.info.keysize+sizeof(SIZE_T))*(b.info.numkeys/2-offset_ptr));
    b.info.numkeys=(b.info.numkeys+1)/2;
    if(offset_ptr==0){
      //Should not happen though...
      return ERROR_INSANE;
    }else{
      b.SetKey(offset_ptr-1,key);
      b.SetPtr(offset_ptr,inputPtr);
    }
    b.Serialize(buffercache,node);
  }else if (offset_ptr==(b.info.numkeys+1)/2+1){
    // If the key to be inserted is the [m/2]th key, which is in neither xl or xr (It's stored in leaf node but is not a key in the current internal node)
    
    //left side have (b.info.numkeys+1)/2+1 ptrs and (b.info.numkeys+1)/2 keys

    // use the original node as the left node
    xl_ptr=node;
    newkey=key;
    // Allocate the right node
    // Allocate a new node. NOTE that AllocateNode cannot take const SIZE_T as an argument.
    AllocateNode(xr_ptr);
    //ptr=superblock.info.freelist;

    if (xr_ptr==0) { 
      return ERROR_NOSPACE;
    }
    BTreeNode xr_node(BTREE_INTERIOR_NODE,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
    xr_node.info.rootnode=b.info.rootnode;
    buffercache->NotifyAllocateBlock(xr_ptr);

    xr_node.info.numkeys=(b.info.numkeys+1)/2;
    memmove(xr_node.ResolveKey(0),b.ResolveKey(offset_ptr-1),(b.info.keysize+b.info.valuesize)*(b.info.numkeys-(offset_ptr-1)));
    xr_node.SetPtr(0,inputPtr);
    b.info.numkeys=(b.info.numkeys+1)/2;
    b.Serialize(buffercache,node);
    xr_node.Serialize(buffercache,xr_ptr);

    
  }else{
    // If the key to be inserted is in xr
    //left side have (b.info.numkeys+1)/2+1 ptrs and (b.info.numkeys+1)/2 keys

    // use the original node as the left node
    xl_ptr=node;
    // the (b.info.numkeys+1)/2+1 th key is the key to be inserted into parent node. Index starts at 0 so its (b.info.numkeys+1)/2
    b.GetKey((b.info.numkeys+1)/2,newkey);//Get the new key
    // Allocate the right node
    // Allocate a new node. NOTE that AllocateNode cannot take const SIZE_T as an argument.
    AllocateNode(xr_ptr);
    //ptr=superblock.info.freelist;

    if (xr_ptr==0) { 
      return ERROR_NOSPACE;
    }
    BTreeNode xr_node(BTREE_INTERIOR_NODE,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
    xr_node.info.rootnode=b.info.rootnode;
    buffercache->NotifyAllocateBlock(xr_ptr);

    xr_node.info.numkeys=(b.info.numkeys+1)/2;
    // void * memmove ( void * destination, const void * source, size_t num );
    memmove(xr_node.ResolvePtr(0),b.ResolvePtr((b.info.numkeys+1)/2+1),sizeof(SIZE_T)+(b.info.keysize+sizeof(SIZE_T))*(offset_ptr-1-((b.info.numkeys+1)/2)));
    if (offset_ptr<b.info.numkeys)
      memmove(xr_node.ResolveKey(offset_ptr-((b.info.numkeys+1)/2+1)),b.ResolveKey(offset_ptr-1),(b.info.keysize+b.info.valuesize)*(b.info.numkeys-(offset_ptr-1)));

    xr_node.SetKey(offset_ptr-((b.info.numkeys+1)/2+1),key);
    xr_node.SetPtr(offset_ptr-((b.info.numkeys+1)/2+1)+1,inputPtr);
    b.info.numkeys=(b.info.numkeys+1)/2;
    b.Serialize(buffercache,node);
    xr_node.Serialize(buffercache,xr_ptr);

  }
  // Now handle the parent node. 
  // Check whether we have enough space to insert a new node.
  // One extra read here can maybe be prevented. But it doesnt matter that much
  rc= parentNode.Unserialize(buffercache,b.info.rootnode);
  if (parentNode.info.GetNumSlotsAsInterior()==parentNode.info.numkeys){
    return SplitInternal(b.info.rootnode, newkey, xr_ptr);
  }
  // If there is enough space, move parent node's key one forward and insert xl and xr
  if (parentNode.info.numkeys==0){
    offset_ptr=1;
  }else{
    offset_ptr=0;
    for (offset=0;offset<parentNode.info.numkeys;offset++) { 
      rc=parentNode.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (newkey<testkey || newkey==testkey) {
        // OK, so we now have the first key that's larger
        // So we record the offset and quit
        offset_ptr=offset+1;
        break;
      }
    }
    if (offset==parentNode.info.numkeys){
      offset_ptr=offset;
    }
    if (offset_ptr==0){
      return ERROR_INSANE;
    }
    if (parentNode.info.numkeys!=offset_ptr){
      // void * memmove ( void * destination, const void * source, size_t num );
      memmove(parentNode.data+sizeof(SIZE_T)+(offset_ptr)*(sizeof(SIZE_T)+parentNode.info.keysize),parentNode.data+sizeof(SIZE_T)+(offset_ptr-1)*(sizeof(SIZE_T)+parentNode.info.keysize),(parentNode.info.keysize+sizeof(SIZE_T))*(parentNode.info.numkeys-offset_ptr));
    }
  }
  parentNode.info.numkeys++;
  parentNode.SetKey(offset_ptr-1,newkey);
  parentNode.SetPtr(offset_ptr-1,xl_ptr);
  parentNode.SetPtr(offset_ptr,xr_ptr);
  parentNode.Serialize(buffercache,b.info.rootnode);
  return ERROR_NOERROR;
}

// The function to split a leaf node
ERROR_T BTreeIndex::SplitLeaf(const SIZE_T &node, const KEY_T &key, const VALUE_T &value){
  // If leaf is already full, then split the leaf.
  // Insert into leaf, pretending that the leaf has space for the new key/value.
  // Split x into 2 new leaves xl and xr. xl contains the l/2 smallest keys and xr contains the remaining l/2 keys. Let J be the minimum key in xr
  // Make a copy of J to be the parent of xl and xr. insert the copy into the parent node. (key=J, value=pointer to xl)

  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  SIZE_T offset_keyvalue;
  KEY_T testkey;
  KeyValuePair testkeyvalue;
  SIZE_T ptr;
  SIZE_T xl_ptr;
  SIZE_T xr_ptr;
  BTreeNode xl_node;
  BTreeNode xr_node;
  BTreeNode parentNode;
  KEY_T xl_key;//The key used in parent node
  KEY_T xr_key;
  VALUE_T xl_value;
  VALUE_T xr_value;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }
  //check whether the node is internal node
  assert(b.info.nodetype==BTREE_LEAF_NODE);
  //check that we indeed have no free space.
  assert(b.info.GetNumSlotsAsLeaf()==b.info.numkeys);
  // Find the position to insert key and value.
  for (offset=0;offset<b.info.numkeys;offset++) { 
    rc=b.GetKey(offset,testkey);
    if (rc) {  return rc; }
    if (key<testkey || key==testkey) {
      // OK, so we now have the first key that's larger
      // So we record the offset and quit
      offset_keyvalue=offset;
      break;
    }
  }
  if (offset==b.info.numkeys){
    offset_keyvalue=b.info.numkeys;
  }
  if (offset_keyvalue<=b.info.numkeys/2){
    // If the key to be inserted is in xl

    // Allocate right node
    rc=CreateLeafNode(xr_ptr, b.info.rootnode, b.ResolveKey(b.info.numkeys/2), b.info.numkeys/2+1);
    b.GetKey(b.info.numkeys/2,xr_key);//Get the new key for right side, which is the smallest key on the right half.
    b.GetVal(b.info.numkeys/2,xr_value);//Get the new value for right side
    if (rc) { return rc; }
    // Do the left node. Use the original node as the left node
    xl_ptr=node;
    // Move the memory after the insertion place one forward.
    // void * memmove ( void * destination, const void * source, size_t num );
    memmove(b.ResolveKey(offset_keyvalue+1),b.ResolveKey(offset_keyvalue),(b.info.keysize+b.info.valuesize)*(b.info.numkeys/2-offset_keyvalue));
    b.info.numkeys=b.info.numkeys/2+1;
    b.SetKey(offset_keyvalue,key);
    b.SetVal(offset_keyvalue,value);
    b.GetKey(0,xl_key);//Get the new key for left side
    b.GetVal(0,xl_value);//Get the new value for left side
    b.Serialize(buffercache,node);
    //No bug until here

  }else{
    // If the key to be inserted is in xr

    // use the original node as the left node
    xl_ptr=node;
    b.GetKey(0,xl_key);//Get the new key for left side, which is the smallest key on the left half.
    b.GetVal(0,xl_value);//Get the new value for left side
    // Allocate the right node
    // Allocate a new node. NOTE that AllocateNode cannot take const SIZE_T as an argument.
    AllocateNode(xr_ptr);
    //ptr=superblock.info.freelist;

    if (xr_ptr==0) { 
      return ERROR_NOSPACE;
    }
    BTreeNode xr_node(BTREE_LEAF_NODE,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
    xr_node.info.rootnode=b.info.rootnode;
    buffercache->NotifyAllocateBlock(xr_ptr);

    // void * memmove ( void * destination, const void * source, size_t num );
    xr_node.info.numkeys=b.info.numkeys/2+1;
    memmove(xr_node.ResolveKey(0),b.ResolveKey(b.info.numkeys/2+1),(b.info.keysize+b.info.valuesize)*(offset_keyvalue-b.info.numkeys/2-1));
    if (offset_keyvalue<b.info.numkeys)
      memmove(xr_node.ResolveKey(offset_keyvalue-b.info.numkeys/2),b.ResolveKey(offset_keyvalue),(b.info.keysize+b.info.valuesize)*(b.info.numkeys-offset_keyvalue));
    
    xr_node.SetKey(offset_keyvalue-b.info.numkeys/2-1,key);
    xr_node.SetVal(offset_keyvalue-b.info.numkeys/2-1,value);
    xr_node.GetKey(0,xr_key);//Get the new key for right side
    xr_node.GetVal(0,xr_value);//Get the new value for right side
    b.info.numkeys=b.info.numkeys/2+1;
    b.Serialize(buffercache,node);
    xr_node.Serialize(buffercache,xr_ptr);

  }
  // Now handle the parent node. 
  // Check whether we have enough space to insert a new node.
  // One extra read here can maybe be prevented. But it doesnt matter that much
  rc= parentNode.Unserialize(buffercache,b.info.rootnode);
  if (parentNode.info.GetNumSlotsAsInterior()==parentNode.info.numkeys){
    return SplitInternal(b.info.rootnode, xr_key, xr_ptr);
  }
  // If there is enough space, move parent node's key one forward and insert xl and xr
  offset_keyvalue=0;
  for (offset=0;offset<parentNode.info.numkeys;offset++) { 
    rc=parentNode.GetKey(offset,testkey);
    if (rc) {  return rc; }
    if (xr_key<testkey || xr_key==testkey) {
      // OK, so we now have the first key that's larger
      // So we record the offset and quit
      offset_keyvalue=offset;
      break;
    }
  }
  if (offset==parentNode.info.numkeys){
    offset_keyvalue=offset;
  }
  if (parentNode.info.numkeys!=offset_keyvalue){
    // void * memmove ( void * destination, const void * source, size_t num );
    memmove(parentNode.data+sizeof(SIZE_T)+(offset_keyvalue+1)*(sizeof(SIZE_T)+parentNode.info.keysize),parentNode.data+sizeof(SIZE_T)+offset_keyvalue*(sizeof(SIZE_T)+parentNode.info.keysize),(parentNode.info.keysize+sizeof(SIZE_T))*(parentNode.info.numkeys-offset_keyvalue));
  }
  parentNode.info.numkeys++;
  parentNode.SetKey(offset_keyvalue,xr_key);
  parentNode.SetPtr(offset_keyvalue,xl_ptr);
  parentNode.SetPtr(offset_keyvalue+1,xr_ptr);
  parentNode.Serialize(buffercache,b.info.rootnode);
  return ERROR_NOERROR;
}


// The helper function for insert. Can be called recursively.
ERROR_T BTreeIndex::InsertHelper(const SIZE_T &node, const KEY_T &key, const VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;
  std::cout<<"Step 2"<<std::endl;
  rc= b.Unserialize(buffercache,node);
  std::cout<<"Step 3. Node: "<<node<<std::endl;
  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
    case BTREE_SUPERBLOCK:
      return InsertHelper(b.info.rootnode, key,value);
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      // Scan through key/ptr pairs
      //and recurse if possible
      for (offset=0;offset<b.info.numkeys;offset++) { 
        rc=b.GetKey(offset,testkey);
        if (rc) {  return rc; }
        if (key<testkey || key==testkey) {
          // OK, so we now have the first key that's larger
          // so we ned to recurse on the ptr immediately previous to 
          // this one, if it exists
          rc=b.GetPtr(offset,ptr);
          if (rc) { return rc; }
          return InsertHelper(ptr, key,value);
        }
      }
      // if we got here, we need to go to the next pointer, if it exists
      if (b.info.numkeys>0) { 
        rc=b.GetPtr(b.info.numkeys,ptr);
        if (rc) { return rc; }
        return InsertHelper(ptr, key,value);
      } else {
        if (b.info.nodetype==BTREE_ROOT_NODE){
          //special situation when the nodee is the root node and has no key
          //Get the stored pointer to the next leaf node.
          rc=b.GetPtr(0,ptr);
          if (rc) { return rc; }
          std::cout<<"Special situation. Root Node: "<<ptr<<std::endl;
          return InsertHelper(ptr, key,value); // The first leaf node is initialized
        }else{
          // There are no keys at all on this node, so nowhere to go
          
          return ERROR_INSANE;
        }
      }
      break;
    case BTREE_LEAF_NODE:
      std::cout<<"Step 4"<<std::endl;
      // Scan through keys looking for matching value
      for (offset=0;offset<b.info.numkeys;offset++) { 
        rc=b.GetKey(offset,testkey);
        if (rc) {  return rc; }
        // we've found the place to insert the key and value
        if (key<testkey || testkey==key) { 
          //WRITE ME
          if (b.info.GetNumSlotsAsLeaf()==b.info.numkeys){
            return SplitLeaf(node,key,value);
          }else{
            //void * memmove ( void * destination, const void * source, size_t num );
            memmove(b.ResolveKey(offset+1),b.ResolveKey(offset),(b.info.keysize+b.info.valuesize)*(b.info.numkeys-offset));
            // Move the memory after the insertion place one forward.
            b.info.numkeys++;
            b.SetKey(offset,key);
            b.SetVal(offset,value);
          }
          b.Serialize(buffercache,node);
          return ERROR_NOERROR;
        }
      }
      //If there is no key that is smaller than the key inserted, check if there is free space.
      if (b.info.GetNumSlotsAsLeaf()==b.info.numkeys){
        return SplitLeaf(node,key,value);
      }else{
        //If there is free space. Then just insert the key and value at the next availble position.
        b.info.numkeys++;
        b.SetKey(offset,key);
        b.SetVal(offset,value);
        b.Serialize(buffercache,node);
        return ERROR_NOERROR;
      }
      
      break;
    default:
      // We can't be looking at anything other than a root, internal, or leaf
      return ERROR_INSANE;
      break;
  }  

  return ERROR_INSANE;
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  // We start at the superblock. 
  // 1. Check if the current block is a leaf node. 
  //     If not a leaf node.
  //        do search (may use linear search for now, can write a separate function)
  //            If currKey<key, then move to the right. Otherwise return the pointer of currKey. (next pointer to the right)
  //            If we reached the end, then the key is larger than any other key in the current node. Insert into the pointer Which leads us to the next block in the linked list
  //     If a leaf node,
  //        If leaf contains <L keys, insert into the leaf at the correct position (using search)
  //        If leaf is already full, then split the leaf.
  //            Insert into leaf, pretending that the leaf has space for the new key/value.
  //            Split x into 2 new leaves xl and xr. xl contains the l/2 smallest keys and xr contains the remaining l/2 keys. Let J be the minimum key in xr
  //            Make a copy of J to be the parent of xl and xr. insert the copy into the parent node. (key=J, value=pointer to xl)
  //        Splitting an internal node:
  //            The difference is xl contains [m-2]-1 smallest keys and xr contains [m/2] largest keys. 
  //            Note that the [m/2]th key J is not placed in xl or xr, but is used to be a key in parent node
  //            Make J the parent of xl and xr, and push j together with its child pointers(to xl) into the parent of x. 
  std::cout<<"Step 1"<<std::endl;
  std::cout<<"displaying node"<<std::endl;
  Display(cerr , BTREE_DEPTH_DOT);

  std::cout<<"displaying ends"<<std::endl;
  // BTreeNode b;
  // ERROR_T rc;
  // SIZE_T ptr;
  // rc=b.Unserialize(buffercache,superblock_index)
  // if (b.)
  // rc=CreateLeafNode(ptr,superblock_index+1);
  // if (rc) { return rc; }
  // // Append the newleaf to the root's first pointer
  // newrootnode.SetPtr(0,ptr);
  return InsertHelper(superblock_index,key,value);
}

  
ERROR_T BTreeIndex::Update(KEY_T key, VALUE_T value)
{
  // WRITE ME
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, value);
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  // this function checks the following condition and returns ERROR_INSANE if any condition is false
  // all paths from the root to a leaf have the same depth
  // at least ceiling((n+1)/2) pointers in any interior node is actually used, where n is the max allowed key num for a block
  // at least floor((n+1)/1) pointers in any leaf node is actually used to point to data records

  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;
  VALUE_T value;
  std::queue<SIZE_T> Q;

  rc= b.Unserialize(buffercache,superblock.info.rootnode);

  if (rc!=ERROR_NOERROR) { return rc; }

  if (b.info.numkeys < 1) {
    return ERROR_NOERROR;
  }
  else {
    // push the 2nd level blocks into Q
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetPtr(offset,ptr);
      if (rc) {  return rc; }
      Q.push(ptr);
    }  
    
    bool terminate = false;
    while (true) {
      // unserialize all nodes in the queue
      // for every node, check if they are the same type
      // check if every node meets saturation rate limit
      // push all their childrens into Q
      // if all nodes are leaf, check if in order, then terminate
      std::set<SIZE_T> s; // contains the types of nodes in a level 
      std::queue<VALUE_T> v; // contains the values of all the leaf nodes
      for (unsigned i=0; i<Q.size(); i++) {
        SIZE_T next = Q.front();  
	Q.pop();
        b.Unserialize(buffercache,next);
        s.insert(b.info.nodetype);
        switch (b.info.nodetype) {
          case BTREE_INTERIOR_NODE:
            // at least ceiling((n+1)/2) pointers in any interior node is actually used, where n is the max allowed key num for a block
            if ((b.info.numkeys < (b.info.blocksize+1)/2+1)) {
              return ERROR_INSANE;
            }
            // push all children into queue
            for (offset=0;offset<b.info.numkeys;offset++) {
              rc = b.GetPtr(offset,ptr);
              if (rc) {return ERROR_INSANE;}
              Q.push(ptr);
            }
          case BTREE_LEAF_NODE:
            // at least floor((n+1)/1) pointers in any leaf node is actually used to point to data records
            if ((b.info.numkeys < (b.info.blocksize+1)/2)) {
              return ERROR_INSANE;
            }
            // push all value into vector
            for (unsigned j=0; j<b.info.numkeys;j++) {
              rc = b.GetVal(j,value);
              if (rc) {return ERROR_INSANE;};
              v.push(value);
            }
            continue;
        }
      }

      // if there are more than 1 type of nodes in a level, tree is insane
      if (s.size() > 1) {
        return ERROR_INSANE;
      }

      // iterate through v and check that values are in order
      while (v.size() > 1) {
	VALUE_T v1 = v.front();
	v.pop();
	VALUE_T v2 = v.front();	
	if (v2 < v1) {
	  return ERROR_INSANE;
        }
      }



    }
  }
  return ERROR_NOERROR;
}
  


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  Display(os, BTREE_DEPTH_DOT);
  return os;
}




