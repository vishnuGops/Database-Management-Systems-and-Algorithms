/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package btree;

import java.io.*;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import btree.*;
/**
 * btfile.java This is the main definition of class BTreeFile, which derives
 * from abstract base class IndexFile. It provides an insert/delete interface.
 */
public class BTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	/**
	 * It causes a structured trace to be written to a file. This output is used
	 * to drive a visualization tool that shows the inner workings of the b-tree
	 * during its operations.
	 *
	 * @param filename
	 *            input parameter. The trace file name
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	/**
	 * Stop tracing. And close trace file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	/**
	 * Access method to data member.
	 * 
	 * @return Return a BTreeHeaderPage object that is the header page of this
	 *         btree file.
	 */
	public BTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno)
			throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty)
			throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	/**
	 * BTreeFile class an index file with given filename should already exist;
	 * this opens it.
	 *
	 * @param filename
	 *            the B+ tree file name. Input parameter.
	 * @exception GetFileEntryException
	 *                can not ger the file from DB
	 * @exception PinPageException
	 *                failed when pin a page
	 * @exception ConstructPageException
	 *                BT page constructor failed
	 */
	public BTreeFile(String filename) throws GetFileEntryException,
			PinPageException, ConstructPageException {

		headerPageId = get_file_entry(filename);

		headerPage = new BTreeHeaderPage(headerPageId);
		dbname = new String(filename);
		/*
		 * 
		 * - headerPageId is the PageId of this BTreeFile's header page; -
		 * headerPage, headerPageId valid and pinned - dbname contains a copy of
		 * the name of the database
		 */
	}

	/**
	 * if index file exists, open it; else create it.
	 *
	 * @param filename
	 *            file name. Input parameter.
	 * @param keytype
	 *            the type of key. Input parameter.
	 * @param keysize
	 *            the maximum size of a key. Input parameter.
	 * @param delete_fashion
	 *            full delete or naive delete. Input parameter. It is either
	 *            DeleteFashion.NAIVE_DELETE or DeleteFashion.FULL_DELETE.
	 * @exception GetFileEntryException
	 *                can not get file
	 * @exception ConstructPageException
	 *                page constructor failed
	 * @exception IOException
	 *                error from lower layer
	 * @exception AddFileEntryException
	 *                can not add file into DB
	 */
	public BTreeFile(String filename, int keytype, int keysize,
			int delete_fashion) throws GetFileEntryException,
			ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 *
	 * @exception PageUnpinnedException
	 *                error from the lower layer
	 * @exception InvalidFrameNumberException
	 *                error from the lower layer
	 * @exception HashEntryNotFoundException
	 *                error from the lower layer
	 * @exception ReplacerException
	 *                error from the lower layer
	 */
	public void close() throws PageUnpinnedException,
			InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 * @exception IteratorException
	 *                iterator error
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception FreePageException
	 *                error when free a page
	 * @exception DeleteFileEntryException
	 *                failed when delete a file from DM
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException,
			UnpinPageException, FreePageException, DeleteFileEntryException,
			ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException,
			IteratorException, PinPageException, ConstructPageException,
			UnpinPageException, FreePageException {

		BTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new BTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page,
					headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage
					.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // BTLeafPage

			unpinPage(pageno);
			freePage(pageno);
		}

	}

	private void updateHeader(PageId newRoot) throws IOException,
			PinPageException, UnpinPageException {

		BTreeHeaderPage header;
		PageId old_data;

		header = new BTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */);

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	/**
	 * insert record with the given key and rid
	 *
	 * @param key
	 *            the key of the record. Input parameter.
	 * @param rid
	 *            the rid of the record. Input parameter.
	 * @exception KeyTooLongException
	 *                key size exceeds the max keysize.
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IOException
	 *                error from the lower layer
	 * @exception LeafInsertRecException
	 *                insert error in leaf page
	 * @exception IndexInsertRecException
	 *                insert error in index page
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception NodeNotMatchException
	 *                node not match index page nor leaf page
	 * @exception ConvertException
	 *                error when convert between revord and byte array
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error when search
	 * @exception IteratorException
	 *                iterator error
	 * @exception LeafDeleteException
	 *                error when delete in leaf page
	 * @exception InsertException
	 *                error when insert in index page
	 */
	public void insert(KeyClass key, RID rid) throws KeyTooLongException,
			KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException,
			UnpinPageException, PinPageException, NodeNotMatchException,
			ConvertException, DeleteRecException, IndexSearchException,
			IteratorException, LeafDeleteException, InsertException,
			IOException

	{
		KeyDataEntry newRootEntry;
		
		//get root id from header if it exists
		PageId pgid =  headerPage.get_rootId();
		
		//check if tree is empty
		//if tree is empty
		if(pgid.pid == INVALID_PAGE)
		{
			//create new root page since there exists no root
			BTLeafPage newRootPage;
			PageId newRootPageId;
			newRootPage = new BTLeafPage(AttrType.attrInteger);
			
			//get the page id of the new root page
			newRootPageId = newRootPage.getCurPage();
			
			//set pointers for the new root page to invalid page
			newRootPage.setNextPage(new PageId(INVALID_PAGE));
			newRootPage.setPrevPage(new PageId(INVALID_PAGE));
			
			//insert the record into new root page
			newRootPage.insertRecord(key, rid);
			
			//unpin the new root page after insertion
			unpinPage(newRootPageId, true);
			
			//update header to new root page id
			updateHeader(newRootPageId);
			
			return;
		
		}
		//if tree is not empty
		else {
			//enter the value into respective page using key
			newRootEntry = _insert(key, rid, headerPage.get_rootId());
			
			//if root split didnt take place
			if(newRootEntry == null)
				return;
			
			//if root split and new root needs to be created
			else if(newRootEntry != null)
			{
				BTIndexPage newRootPage;
				PageId newRootPageId;
				
				newRootPage = new BTIndexPage(AttrType.attrInteger);
				newRootPageId = newRootPage.getCurPage();
				
				//insert the copyUp key into new root with its child pointer
				PageId childPtr = ((IndexData)newRootEntry.data).getData();
				newRootPage.insertKey(newRootEntry.key, childPtr);
				
				//set the previous page pointer to old root 
				newRootPage.setPrevPage(headerPage.get_rootId());
				
				unpinPage(newRootPageId, true);
				//update header to the new root page
				updateHeader(newRootPageId);
				
			}
		}
		
		return;
	}

	private KeyDataEntry _insert(KeyClass key, RID rid, PageId currentPageId)
			throws PinPageException, IOException, ConstructPageException,
			LeafDeleteException, ConstructPageException, DeleteRecException,
			IndexSearchException, UnpinPageException, LeafInsertRecException,
			ConvertException, IteratorException, IndexInsertRecException,
			KeyNotMatchException, NodeNotMatchException, InsertException

	{
		//create a page to check page type
		BTSortedPage curPage;
		Page p;
		
		KeyDataEntry copyUp;
		
		//pin the Page
		p=pinPage(currentPageId);
		curPage = new BTSortedPage(p, AttrType.attrInteger);
		
		//check the type
		//if page type is leaf page then
		if(curPage.getType() == NodeType.LEAF)
		{
			BTLeafPage curLeafPage = new BTLeafPage(p, AttrType.attrInteger);
			PageId curLeafPageId = currentPageId;
			
			//check if there is enough space to insert the key
			if(curLeafPage.available_space() >= BT.getKeyDataLength(key, NodeType.LEAF))
			{
				//insert record into space available
				curLeafPage.insertRecord(key, rid);
				
				unpinPage(curLeafPageId, true);
				
				return null;
			
			}
			//if there is not enough space then split leaf
			else
			{
				//create new leafpage to accomodate record
				BTLeafPage newLeaf;
				PageId newLeafId;
				
				newLeaf = new BTLeafPage(AttrType.attrInteger);
				newLeafId = newLeaf.getCurPage();
				
				//set Pointers to new Leaf page
				newLeaf.setNextPage(new PageId(INVALID_PAGE));
				newLeaf.setPrevPage(curLeafPageId);
				
				curLeafPage.setNextPage(newLeafId);
				
				//use tmp buffer value
				KeyDataEntry tmp;
							
				//move all current leaf record to new leaf
				RID r= new RID();
				for(tmp=curLeafPage.getFirst(r); tmp != null; tmp = curLeafPage.getFirst(r))
				{
					//child will hold the rid of the record
					RID child = ((LeafData) tmp.data).getData();
					newLeaf.insertRecord(tmp.key,child);
					curLeafPage.deleteSortedRecord(r);
					
				}
				
				//check available space in both leaf nodes to balance split
				for(tmp= newLeaf.getFirst(r); newLeaf.available_space() < curLeafPage.available_space(); tmp=newLeaf.getFirst(r))
				{
					RID child = ((LeafData) tmp.data).getData();
					curLeafPage.insertRecord(tmp.key, child); 
					newLeaf.deleteSortedRecord(r);
					
				}
				
				//compare key value to check where to place the key
				if(BT.keyCompare(key, (newLeaf.getFirst(r)).key) >=0)
				{
					newLeaf.insertRecord(key, rid);					
				}
				else
				{
					curLeafPage.insertRecord(key,rid);
				}
				
				//unpin the current leaf page
				unpinPage(curLeafPageId, true);
				
				//copy up the first value from new leaf page to index page
				tmp=newLeaf.getFirst(r);
				copyUp=new KeyDataEntry(tmp.key, newLeafId);
				
				//unpin the new leaf page
				unpinPage(newLeafId, true);
				return copyUp;
			}
		}
		//if page type is index page then 
		else if(curPage.getType()==NodeType.INDEX)
		{
			BTIndexPage curIndex = new BTIndexPage(p, AttrType.attrInteger);
			//set current index page id to current page id
			PageId curIndexId = currentPageId;
			PageId nextPageId;
			
			
			//get Page no of the next index page
			nextPageId = curIndex.getPageNoByKey(key);
			
			unpinPage(curIndexId);
			
			//insert the value in the next page
			copyUp= _insert(key, rid, nextPageId);
			
			//if there is no root split then new root not created
			if(copyUp==null)
				return null;
			
			//else pin the current page
			Page p1 = pinPage(currentPageId);
			curIndex= new BTIndexPage(p1, AttrType.attrInteger);
			
			//check if current index page has available space to hold the key
			if(curIndex.available_space() >= BT.getKeyDataLength(copyUp.key, NodeType.INDEX))
			{
				//if yes then store value in current index page
				//child will hold the child pointer of the key
				PageId child = ((IndexData) copyUp.data).getData();
				curIndex.insertKey(copyUp.key, child);
				
				unpinPage(curIndexId, true);
				
				return null;
			}
			
			//if there isnt enough space in current index node then create new index node and split
			else
			{
				//create new index page
				BTIndexPage newIndex;
				PageId newIndexId;
				KeyDataEntry tmp;
				RID r = new RID();
				RID f = new RID();
				
				//create new index page to split values
				newIndex = new BTIndexPage(AttrType.attrInteger);
				newIndexId = newIndex.getCurPage();
				
				//copy all values in current index page to new index page
				for(tmp = curIndex.getFirst(r); tmp != null; tmp = curIndex.getFirst(r))
				{
					//child holds the child pointer
					PageId child = ((IndexData)tmp.data).getData();
					newIndex.insertKey(tmp.key, child);
					curIndex.deleteSortedRecord(r);
					
				}

				//balance the current index page and index page to split 
				for(tmp = newIndex.getFirst(f);(curIndex.available_space() > newIndex.available_space()); tmp=newIndex.getFirst(f))
				{
					PageId child = ((IndexData)tmp.data).getData();
					curIndex.insertKey(tmp.key, child);
					newIndex.deleteSortedRecord(f);
					
				}
						
				//compare the key with first value in new index to determine where to insert key and child pointer
				tmp = newIndex.getFirst(f);
				PageId childPtr;
				if(BT.keyCompare( copyUp.key, tmp.key) >=0)
				{
					childPtr = ((IndexData)copyUp.data).getData();
					newIndex.insertKey(copyUp.key, childPtr);
				}
				else
				{
					//insert the new index key and child pointer in the old index page
					childPtr = ((IndexData)copyUp.data).getData();
					curIndex.insertKey( copyUp.key, childPtr);
					
				}
				//unpin the current index page
				unpinPage(curIndexId, true);
				
				//move the first value in the new index page to copyup
				copyUp = newIndex.getFirst(r);
				
				//set child pointer of first record as the previous page pointer
				PageId c = ((IndexData)copyUp.data).getData();
				newIndex.setPrevPage(c);
				
				//delete the first record which is moved up to new root
				newIndex.deleteSortedRecord(r);
				
				//copyup will be the new entry into the new root page after index split
				((IndexData)copyUp.data).setData(newIndexId);
				
				//unpin the new root
				unpinPage(newIndexId, true);
				
				return copyUp;

			}
		}
		
		else
		{
			return null;
		}
		
		
		
	}

	



	/**
	 * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry;
	 * it is not the id of the data entry)
	 *
	 * @param key
	 *            the key in pair <key, rid>. Input Parameter.
	 * @param rid
	 *            the rid in pair <key, rid>. Input Parameter.
	 * @return true if deleted. false if no such record.
	 * @exception DeleteFashionException
	 *                neither full delete nor naive delete
	 * @exception LeafRedistributeException
	 *                redistribution error in leaf pages
	 * @exception RedistributeException
	 *                redistribution error in index pages
	 * @exception InsertRecException
	 *                error when insert in index page
	 * @exception KeyNotMatchException
	 *                key is neither integer key nor string key
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception IndexInsertRecException
	 *                error when insert in index page
	 * @exception FreePageException
	 *                error in BT page constructor
	 * @exception RecordNotFoundException
	 *                error delete a record in a BT page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception IndexFullDeleteException
	 *                fill delete error
	 * @exception LeafDeleteException
	 *                delete error in leaf page
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error in search in index pages
	 * @exception IOException
	 *                error from the lower layer
	 *
	 */
	public boolean Delete(KeyClass key, RID rid) throws DeleteFashionException,
			LeafRedistributeException, RedistributeException,
			InsertRecException, KeyNotMatchException, UnpinPageException,
			IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException,
			IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException,
			IOException {
		if (headerPage.get_deleteFashion() == DeleteFashion.NAIVE_DELETE)
			return NaiveDelete(key, rid);
		else
			throw new DeleteFashionException(null, "");
	}

	/*
	 * findRunStart. Status BTreeFile::findRunStart (const void lo_key, RID
	 * *pstartrid)
	 * 
	 * find left-most occurrence of `lo_key', going all the way left if lo_key
	 * is null.
	 * 
	 * Starting record returned in *pstartrid, on page *pppage, which is pinned.
	 * 
	 * Since we allow duplicates, this must "go left" as described in the text
	 * (for the search algorithm).
	 * 
	 * @param lo_key find left-most occurrence of `lo_key', going all the way
	 * left if lo_key is null.
	 * 
	 * @param startrid it will reurn the first rid =< lo_key
	 * 
	 * @return return a BTLeafPage instance which is pinned. null if no key was
	 * found.
	 */

	BTLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException,
			IteratorException, KeyNotMatchException, ConstructPageException,
			PinPageException, UnpinPageException {
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyDataEntry curEntry;

		pageno = headerPage.get_rootId();

		if (pageno.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by
			// startrid =INVALID_PAGEID ; // the caller
			return pageLeaf;
		}

		page = pinPage(pageno);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + pageno + lineSep);
			trace.flush();
		}

		// ASSERTION
		// - pageno and sortPage is the root of the btree
		// - pageno and sortPage valid and pinned

		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startrid);
			while (curEntry != null && lo_key != null
					&& BT.keyCompare(curEntry.key, lo_key) < 0) {

				prevpageno = ((IndexData) curEntry.data).getData();
				curEntry = pageIndex.getNext(startrid);
			}

			unpinPage(pageno);

			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());

			if (trace != null) {
				trace.writeBytes("VISIT node " + pageno + lineSep);
				trace.flush();
			}

		}

		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());

		curEntry = pageLeaf.getFirst(startrid);
		while (curEntry == null) {
			// skip empty leaf pages off to left
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno);
			if (nextpageno.pid == INVALID_PAGE) {
				// oops, no more records, so set this scan to indicate this.
				return null;
			}

			pageno = nextpageno;
			pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startrid);
		}

		// ASSERTIONS:
		// - curkey, curRid: contain the first record on the
		// current leaf page (curkey its key, cur
		// - pageLeaf, pageno valid and pinned

		if (lo_key == null) {
			return pageLeaf;
			// note that pageno/pageLeaf is still pinned;
			// scan will unpin it when done
		}

		while (BT.keyCompare(curEntry.key, lo_key) < 0) {
			curEntry = pageLeaf.getNext(startrid);
			while (curEntry == null) { // have to go right
				nextpageno = pageLeaf.getNextPage();
				unpinPage(pageno);

				if (nextpageno.pid == INVALID_PAGE) {
					return null;
				}

				pageno = nextpageno;
				pageLeaf = new BTLeafPage(pinPage(pageno),
						headerPage.get_keyType());

				curEntry = pageLeaf.getFirst(startrid);
			}
		}

		return pageLeaf;
	}

	/*
	 * Status BTreeFile::NaiveDelete (const void *key, const RID rid)
	 * 
	 * Remove specified data entry (<key, rid>) from an index.
	 * 
	 * We don't do merging or redistribution, but do allow duplicates.
	 * 
	 * Page containing first occurrence of key `key' is found for us by
	 * findRunStart. We then iterate for (just a few) pages, if necessary, to
	 * find the one containing <key,rid>, which we then delete via
	 * BTLeafPage::delUserRid.
	 */

	private boolean NaiveDelete(KeyClass key, RID rid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException,
			ConstructPageException, IOException, UnpinPageException,
			PinPageException, IndexSearchException, IteratorException 
	{
		BTLeafPage leafpage;
		RID tmprid = new RID();
		PageId nextpage;
		KeyDataEntry record;
		
		
		//we use findrunstart to find the first page with key and rid
		leafpage = findRunStart(key, tmprid);
		//check if leaf page exists
		if (leafpage== null)
			//if it doesnt then return false
			return false;
		
		//else use the getcurrent to get the record in the page
		record = leafpage.getCurrent(tmprid);
		
		while(true)
		{
			//check if the entry doesnt exist i.e null
			while(record == null)
			{
				//if its null then check the adjacent pages using getNextPage for the record
				nextpage = leafpage.getNextPage();
				unpinPage(leafpage.getCurPage());
				//if page is INVALID_PAGE then there are no more pages
				if(nextpage.pid == INVALID_PAGE)
				{
					return false;
				}
				else
				{
					//else go to next page 
					Page p;
					RID r = new RID();
					//pin the next page and get first record
					p= pinPage(nextpage);
					leafpage = new BTLeafPage(p, AttrType.attrInteger);
					record = leafpage.getFirst(r);
				}
			}
			
			//compare key value with entry value of leaf page to determine where to find record
			//if key is found on the page then delete
			if(BT.keyCompare(key, record.key)<=0)
			{
				KeyDataEntry k = new KeyDataEntry(key, rid);
				Page p1;
				//delete the found key
				if(leafpage.delEntry(k)==true)
				{
					unpinPage(leafpage.getCurPage(), true);
					return true;
				}
				else
				{
					//if record not found then search in next page
					nextpage = leafpage.getNextPage();
					unpinPage(leafpage.getCurPage());
					p1= pinPage(nextpage);
					leafpage = new BTLeafPage(p1, AttrType.attrInteger);
					record = leafpage.getCurrent(tmprid);
				}
			}
			else
			{
				break;
			}
			
		}
		
		unpinPage(leafpage.getCurPage(), true);
		return false;
					
	}
	
	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null
	 * scan the whole index (2) lo_key = null, hi_key!= null range scan from min
	 * to the hi_key (3) lo_key!= null, hi_key = null range scan from the lo_key
	 * to max (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match (
	 * might not unique) (5) lo_key!= null, hi_key!= null, lo_key < hi_key range
	 * scan from lo_key to hi_key
	 *
	 * @param lo_key
	 *            the key where we begin scanning. Input parameter.
	 * @param hi_key
	 *            the key where we stop scanning. Input parameter.
	 * @exception IOException
	 *                error from the lower layer
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception UnpinPageException
	 *                error when unpin a page
	 */
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
			throws IOException, KeyNotMatchException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}

		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.bfile = this;

		// this sets up scan at the starting position, ready for iteration
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}

	void trace_children(PageId id) throws IOException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {

			BTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new BTSortedPage(pinPage(id), headerPage.get_keyType());

			// Now print all the child nodes of the page.
			if (sortedPage.getType() == NodeType.INDEX) {
				BTIndexPage indexPage = new BTIndexPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				BTLeafPage leafPage = new BTLeafPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}

}
