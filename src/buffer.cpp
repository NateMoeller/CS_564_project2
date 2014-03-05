/**
 * File: buffer.cpp
 * TimeStamp: 8:44pm 2/20/2013
 * Description: This file implements the methods necessary to maintain a buffer manager.
 * Student Name: Nathan Moeller
 * UW Campus Id: 906 463 0339
 * email: ndmoeller@wisc.edu
 *
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

FrameId fid; /* A FrameId that is used for passing the current frameId between functions*/


/**
* Constructor of BufMgr class
*/
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  	bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  	hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

	numBufs = bufs;
  	clockHand = bufs - 1;
}

/**
* Destructor of BufMgr class
*/
BufMgr::~BufMgr() {
	for(FrameId i = 0; i < numBufs; i++){
		if(bufDescTable[i].dirty){
			flushFile(bufDescTable[i].file);
		}
	}

	delete [] bufPool;
	delete [] bufDescTable;
	delete hashTable;
}


/**
* Delete page from file and also from buffer pool if present.
* Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
*
* @param file   File object
* @param PageNo  Page number
*/
void BufMgr::disposePage(File * file, const PageId PageNo){
	for(FrameId i = 0; i < numBufs; i++){
		if(file == bufDescTable[i].file){
			if(PageNo == bufDescTable[i].pageNo){
				if(bufDescTable[i].pinCnt > 0){
					throw PagePinnedException("pagePinned", PageNo, i);
				}
				bufDescTable[i].Clear();
				hashTable->remove(file, PageNo);
			}
		}

	}
	file->deletePage(PageNo);
}


/**
* Advance clock to next frame in the buffer pool
*/
void BufMgr::advanceClock()
{
	clockHand++;
	clockHand = clockHand % (numBufs);
}

/**
* Allocate a free frame.  
*
* @param frame   	Frame reference, frame ID of allocated frame returned via this variable
* @throws BufferExceededException If no such buffer is found which can be allocated
*/
void BufMgr::allocBuf(FrameId & frame) 
{
	bool found = false;
	FrameId starting = clockHand;
	//Loop through the buffer pool until you find a valid frame to replace
	while(!found){
		advanceClock();
		if(bufDescTable[clockHand].valid){
			if(!bufDescTable[clockHand].refbit){
				if(bufDescTable[clockHand].pinCnt == 0){
					//we have found the correct frame
					found = true;
					hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo); //remove from hash table
					if(bufDescTable[clockHand].dirty){
						//write page to disk.
						bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
					}
					else{
						bufDescTable[clockHand].Clear();
					}
					frame = bufDescTable[clockHand].frameNo;
				}
				else{
					if(starting == clockHand && !found){
						//we looped all the way around the buffer pool, throw an exception
						throw BufferExceededException();
					}
				}
			}
			else{
				//clear refbit
				bufDescTable[clockHand].refbit = false;
			}	
		}
		else{
			found = true;
			frame = bufDescTable[clockHand].frameNo;
		}
	}
	bufDescTable[clockHand].Clear();
}

/**
* Reads the given page from the file into a frame and returns the pointer to page.
* If the requested page is already present in the buffer pool pointer to that frame is returned
* otherwise a new frame is allocated from the buffer pool for reading the page.
*
* @param file   	File object
* @param PageNo  Page number in the file to be read
* @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	bool excepThrown = false;
	try{
		hashTable->lookup(file, pageNo, fid);
	}
	catch(HashNotFoundException e){
		excepThrown = true;
		allocBuf(fid);
		Page tPage = file->readPage(pageNo);
		bufPool[fid] = tPage;
		hashTable->insert(file, pageNo, fid);
		bufDescTable[fid].Set(file, pageNo);
	}
	if(!excepThrown){
		bufDescTable[fid].refbit = true;
		bufDescTable[fid].pinCnt++;
	}
	page = &bufPool[fid];
}

/**
* Unpin a page from memory since it is no longer required for it to remain in memory.
*
* @param file   	File object
* @param PageNo  Page number
* @param dirty		True if the page to be unpinned needs to be marked dirty	
* @throws  PageNotPinnedException If the page is not already pinned
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	bool thrown = false;
	try{
		hashTable->lookup(file, pageNo, fid);
	}
	catch(HashNotFoundException e){
		thrown = true;
	}
	if(!thrown){
		if(bufDescTable[fid].pinCnt == 0){
			throw PageNotPinnedException("pageNotPinned", bufDescTable[fid].pageNo, fid);
		}
		else{
			bufDescTable[fid].pinCnt--;
 			if(dirty){
				bufDescTable[fid].dirty = true;
			}
		}
	}


}


/**
* Writes out all dirty pages of the file to disk.
* All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
* Otherwise Error returned.
*
* @param file   	File object
* @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
* @throws BadBufferException If any frame allocated to the file is found to be invalid
*/
void BufMgr::flushFile(const File* file) 
{
	for(FrameId i = 0; i < numBufs; i++){
		if(bufDescTable[i].file == file){
			if(bufDescTable[i].pinCnt != 0){
				//page is already pinned, throw an exception
				throw PagePinnedException("pagePinned", bufDescTable[i].pageNo, i);
			}
			if(!bufDescTable[i].valid){
				//Not a valid page, throw an exception
				throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}

			if(bufDescTable[i].dirty){
				//write to disk if the frame is dirty
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
	}
}


/**
* Allocates a new, empty page in the file and returns the Page object.
* The newly allocated page is also assigned a frame in the buffer pool.
*
* @param file   	File object
* @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
* @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
*/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	Page newPage = file->allocatePage();
	allocBuf(fid);
	bufPool[fid] = newPage;

	page = &bufPool[fid];
	pageNo = bufPool[fid].page_number();

	hashTable->insert(file, pageNo, fid);
	bufDescTable[fid].Set(file, pageNo);
}


/**
* Print member variable values. 
*/
void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
