#include <assert.h>
#include <string.h> //Used for memmove
#include <iostream>
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
    newsuperblock.info.freelist=superblock_index+2;
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
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
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


// The function to create a new leaf node
ERROR_T BTreeIndex::CreateLeafNode(const SIZE_T &ptr){
  BTreeNode node;
  ERROR_T rc;
  // Allocate a new node.
  rc=AllocateNode(ptr);
  if (rc) { return rc; }
  rc=node.Unserialize(buffercache,ptr);
  if (rc) { return rc; }
  node.info.nodetype=BTREE_LEAF_NODE;
  return ERROR_NOERROR;
}

// The function to split an internal node. The input is the node, the key and the value to be inserted after/during the split
ERROR_T BTreeIndex::SplitInternal(const SIZE_T &node, const KEY_T &key, const VALUE_T &value){
  //        Splitting an internal node:
  //            The difference is xl contains [m-2]-1 smallest keys and xr contains [m/2] largest keys. 
  //            Note that the [m/2]th key J is not placed in xl or xr, but is used to be a key in parent node
  //            Make J the parent of xl and xr, and push j together with its child pointers(to xl) into the parent of x. 
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

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }
  //check whether the node is internal node
  assert(b.info.nodetype==BTREE_ROOT_NODE || b.info.nodetype==BTREE_INTERIOR_NODE);
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
  if (offset_keyvalue<b.info.numkeys/2-1){
    // If the key to be inserted is in xl
    rc=b.GetPtr(offset,ptr);
    if (rc) { return rc; }
    // Allocate a new node. (The left node)
    rc=AllocateNode(xl_ptr);
    if (rc) { return rc; }
    rc=xl_node.Unserialize(buffercache,xl_ptr);
    if (rc) { return rc; }
    // Loop through the original node and insert the first [m-2]-1 smallest key/value pairs
    for (offset=0;offset<offset_keyvalue;offset++) { 
      rc=b.GetKeyVal(offset,testkeyvalue);
      if (rc) {  return rc; }
      xl_node.SetKeyVal(offset,testkeyvalue);
    }
    //insert the input key/value pair
    xl_node.SetKey(offset_keyvalue,key);
    xl_node.SetVal(offset_keyvalue,value);
    // Keep doing the loop and insert the first [m-2]-1 smallest key/value pairs
    for (offset=offset_keyvalue;offset<b.info.numkeys/2-1;offset++) { 
      rc=b.GetKeyVal(offset,testkeyvalue);
      if (rc) {  return rc; }
      xl_node.SetKeyVal(offset+1,testkeyvalue);
    }

    //allocate right node
    // Allocate a new node. (The left node)
    rc=AllocateNode(xr_ptr);
    if (rc) { return rc; }
    rc=xl_node.Unserialize(buffercache,xr_ptr);
    if (rc) { return rc; }
    for (offset=b.info.numkeys/2+1;offset<b.info.numkeys;offset++){
      rc=b.GetKeyVal(offset,testkeyvalue);
      if (rc) {  return rc; }
      xr_node.SetKeyVal(offset-(b.info.numkeys/2+1),testkeyvalue);
    }
    //deallocate original node
    rc=DeallocateNode(node);
    if (rc) { return rc; }

  }else if (offset_keyvalue==b.info.numkeys/2-1){
    // If the key to be inserted is the [m/2]th key, which is in neither xl or xr (It's stored in leaf node but is not a key in the current internal node)
  }else{
    // If the key to be inserted is in xr
  }
  return ERROR_UNIMPL;
}

// The function to split a leaf node
ERROR_T BTreeIndex::SplitLeaf(const SIZE_T &node, const KEY_T &key, const VALUE_T &value){

  return ERROR_UNIMPL;
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
  std::cout<<"Step 3"<<std::endl;
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
          return InsertHelper(ptr, key,value);
        }
      }
      // if we got here, we need to go to the next pointer, if it exists
      if (b.info.numkeys>0) { 
        rc=b.GetPtr(b.info.numkeys,ptr);
        if (rc) { return rc; }
        return InsertHelper(ptr, key,value);
      } else {
        // There are no keys at all on this node, so nowhere to go
        // Therefore we need to create a new leaf node and insert that.
        rc=CreateLeafNode(ptr);
        if (rc) { return rc; }
        BTreeNode newLeaf;
        rc= newLeaf.Unserialize(buffercache,ptr);
        if (rc) { return rc; }
        newLeaf.SetKey(0,key);
        newLeaf.SetVal(0,value);
        return ERROR_NOERROR;
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
          if (b.info.GetNumSlotsAsLeaf()==0){
            SplitLeaf(node,key,value);
          }else{
            // Move the memory after the insertion place one forward.
            memmove(b.data+(b.info.keysize+b.info.valuesize)*offset+sizeof(SIZE_T),b.data+(b.info.keysize+b.info.valuesize)*(offset+1)+sizeof(SIZE_T),b.info.GetNumDataBytes()-sizeof(SIZE_T)-(b.info.keysize+b.info.valuesize)*offset);
            b.SetKey(offset,key);
            b.SetVal(offset,value);
          }
          return ERROR_NOERROR;
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
  return InsertHelper(superblock_index+1,key,value);
}

  
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  return ERROR_UNIMPL;
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
  return ERROR_UNIMPL;
}
  


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  return os;
}




