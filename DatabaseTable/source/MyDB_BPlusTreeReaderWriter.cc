
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"
#include <iostream>

using namespace std;

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	int rootPageId = getTable()->lastPage();
	int leafPageId = rootPageId + 1;
	getTable()->setLastPage(leafPageId);

	// create a new root page
	MyDB_PageReaderWriter rootPage = (*this)[rootPageId];
	rootPage.clear();
	rootPage.setType(MyDB_PageType :: DirectoryPage);

	// append a new internal record with key = infinity
	MyDB_INRecordPtr firstInRecord = getINRecord();
	rootPage.append(firstInRecord);
	// set the pointer of the new in record to the new leaf page 
	firstInRecord->setPtr(leafPageId);

	// update the root location
	getTable()->setRootLocation(rootPageId);
	rootLocation = getTable()->getRootLocation();

	// create a new empty leaf page
	MyDB_PageReaderWriter leafPage = (*this)[leafPageId];
	leafPage.clear();
	leafPage.setType(MyDB_PageType :: RegularPage);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	return nullptr;
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	return nullptr;
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int curPageNode, vector <MyDB_PageReaderWriter> &, MyDB_AttValPtr, MyDB_AttValPtr) {
	return false;
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {
	// cout << "append here\n";
	MyDB_RecordPtr appendedInRec = append(rootLocation, appendMe);
	if (appendedInRec != nullptr) {
		getTable()->setLastPage(getTable()->lastPage() + 1);
		MyDB_PageReaderWriter newRootPage = last();
		newRootPage.clear();
		newRootPage.setType(MyDB_PageType :: DirectoryPage);


		MyDB_INRecordPtr appendNew = getINRecord();
		appendNew->setPtr(rootLocation);
		newRootPage.append(appendedInRec);
		newRootPage.append(appendNew);

		getTable()->setRootLocation(getTable()->lastPage());
		rootLocation = getTable()->getRootLocation();
	}
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
	if(splitMe.append(andMe)) {
		return nullptr;
	}

	MyDB_RecordPtr lhs, rhs, tempRec, tempRec2;
	if (splitMe.getType() == MyDB_PageType :: RegularPage) {
		lhs = getEmptyRecord();
		rhs = getEmptyRecord();
		tempRec = getEmptyRecord();
		tempRec2 = getEmptyRecord();
	} else {
		lhs = getINRecord();
		rhs = getINRecord();
		tempRec = getINRecord();
		tempRec2 = getINRecord();
	}

	splitMe.sortInPlace(buildComparator(lhs, rhs), lhs, rhs);

	MyDB_RecordIteratorPtr slow = splitMe.getIterator(tempRec), fast = splitMe.getIterator(tempRec2);
	// cout << "1\n";
	getTable()->setLastPage(getTable()->lastPage() + 1);
	MyDB_PageReaderWriter newSmallerPage = last(), tempLargerPage(*getBufferMgr());

	if (splitMe.getType() != MyDB_PageType :: RegularPage) {
		splitMe.append(getINRecord());
	}

	while (fast->hasNext()) {
		fast->getNext();
		if (fast->hasNext())
			fast->getNext();
		slow->getNext();
		newSmallerPage.append(tempRec);
	}
	// cout << "2\n";
	if (buildComparator(andMe, tempRec)()) {
		newSmallerPage.append(andMe);
	} else {
		tempLargerPage.append(andMe);
	}
	// cout << "3\n";
	while (slow->hasNext()) {
		slow->getNext();
		tempLargerPage.append(tempRec);
	}
	// cout << "4\n";
	splitMe.clear();
	MyDB_RecordIteratorPtr iterateToMe = tempLargerPage.getIterator(tempRec);
	while (iterateToMe->hasNext()) {
		iterateToMe->getNext();
		splitMe.append(tempRec);
	}
	// cout << "5\n";
	splitMe.sortInPlace(buildComparator(lhs, rhs), lhs, rhs);
	newSmallerPage.sortInPlace(buildComparator(lhs, rhs), lhs, rhs);
	// cout << "6\n";
	iterateToMe = splitMe.getIterator(tempRec);
	iterateToMe->getNext();
	// cout << "7\n";
	MyDB_INRecordPtr newInRecord = getINRecord();
	newInRecord->setKey(getKey(tempRec));
	newInRecord->setPtr(getTable()->lastPage());
	// cout << "8\n";
	return newInRecord;

	// vector<MyDB_RecordPtr> allRecords;
	// MyDB_RecordPtr tempRecord = getINRecord();
	// MyDB_RecordIteratorPtr iter = splitMe.getIterator(tempRecord);
	// while(iter->hasNext()) {
	// 	iter->getNext();
	// 	allRecords.push_back(make_shared<MyDB_Record>(*tempRecord));
	// }
	// allRecords.push_back(make_shared<MyDB_Record>(*andMe));

	// sort(allRecords.begin(), allRecords.end(), [this](MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	// 	return buildComparator(lhs, rhs)();
	// });

	// int midIndex = allRecords.size() / 2;

	// getTable()->setLastPage(getTable()->lastPage() + 1);
	// // allocate a new page for the upper half of the records
	// MyDB_PageReaderWriter newPage = (*this)[getTable()->lastPage()];

	// for(int i = 0; i < midIndex; i++) {
	// 	newPage.append(allRecords[i]);
	// }

	// splitMe.clear();

	// for(int i = midIndex; i < allRecords.size(); i++) {
	// 	splitMe.append(allRecords[i]);
	// }

	// MyDB_INRecordPtr newInternalRecord = getINRecord();

	// newInternalRecord->setKey(getKey(allRecords[midIndex]));
	// newInternalRecord->setPtr(getTable()->lastPage());

	// return newInternalRecord;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe) {
	// cout << endl << whichPage << ", " << (*this)[whichPage].getType() << endl;
	MyDB_PageReaderWriter curPageNode = (*this)[whichPage];
	if (curPageNode.getType() == MyDB_PageType :: RegularPage) {
		// reach leaf page, append appendMe
		if (!curPageNode.append(appendMe)) {
			// leaf page is full, need to split
			MyDB_RecordPtr splitedInRecord = split(curPageNode, appendMe);
			return splitedInRecord;
		}
	} else {
		// sort internal page before iterate
		MyDB_INRecordPtr lhs = getINRecord(), rhs = getINRecord();
		curPageNode.sortInPlace(buildComparator(lhs, rhs), lhs, rhs);

		// // iterate all inRecords in curPageNode and recursively append
		MyDB_INRecordPtr iterateToMe = getINRecord();
		MyDB_RecordIteratorPtr pageRecIter = curPageNode.getIterator(iterateToMe);
		while (pageRecIter->hasNext()) {
			// compare inRecord's attribute with appendMe's attribute, if appendMe's smaller, recursively append appendMe
			pageRecIter->getNext();
			auto cmp = buildComparator(appendMe, iterateToMe);
			if (cmp()) {
				MyDB_RecordPtr appendedInRecord = append(iterateToMe->getPtr(), appendMe);
				if (appendedInRecord != nullptr) {
					MyDB_RecordPtr splitedInRecord = split(curPageNode, appendMe);
					return splitedInRecord;
				}
			}
		}
	}
	// printTree();
	return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
	printNode(rootLocation, 0);
	// vector<vector<vector<int>>> v;
	// printTreeLevel(v, 0, rootLocation);
	// for (auto v1 : v) {
	// 	for (auto v2 : v1) {
	// 		cout << "[";
	// 		for (auto att : v2) {
	// 			cout << att << ", ";
	// 		}
	// 		cout << "], ";
	// 	}
	// 	cout << endl;
	// }
}

void MyDB_BPlusTreeReaderWriter :: printTreeLevel(vector<vector<vector<int>>>& v, int level, int cur) {
	MyDB_PageReaderWriter curPage = (*this)[cur];
	if (v.size() <= level) {
		v.push_back(vector<vector<int>>());
	}

	if (curPage.getType() == RegularPage) {
		MyDB_RecordPtr tempRec = getEmptyRecord();
		MyDB_RecordIteratorPtr iterateToMe = curPage.getIterator(tempRec);
		vector<int> tempVec;
		while (iterateToMe->hasNext()) {
			iterateToMe->getNext();
			tempVec.push_back(1);
		}
		v[level].push_back(tempVec);
	} else {
		MyDB_INRecordPtr tempRec = getINRecord();
		MyDB_RecordIteratorPtr iterateToMe = curPage.getIterator(tempRec);
		while (iterateToMe->hasNext()) {
			iterateToMe->getNext();
			v[level].push_back(vector<int>({1}));
			printTreeLevel(v, level + 1, tempRec->getPtr());
		}
	}
}

void MyDB_BPlusTreeReaderWriter :: printNode(int pageNum, int level) {
	MyDB_PageReaderWriter currentNode = (*this)[pageNum];
	
	MyDB_RecordIteratorPtr iter = currentNode.getIterator(getEmptyRecord());

	if(currentNode.getType() == DirectoryPage) {
		cout << string(level * 4, ' ') << "Internal Node at Level " << level << ": ";
		while(iter->hasNext()) {
			iter->getNext();
			MyDB_INRecordPtr internalRecord = getINRecord();
			cout << "[Key: " << internalRecord->getKey()->toString() 
				 << ", Ptr: " << internalRecord->getPtr() << "] "; 
		}
		cout<<endl;

		iter = currentNode.getIterator(getEmptyRecord());
		// recursively print the child nodes
		while (iter->hasNext()) {
			iter->getNext();
			MyDB_INRecordPtr internalRecord = getINRecord();
			printNode(internalRecord->getPtr(), level + 1);
		}
	} else {
		// leaf nodes
		cout << string(level * 4, ' ') << "Leaf Node at Level " << level << ": ";
		
		while (iter->hasNext()) {
			iter->getNext();
			MyDB_RecordPtr leafRecord = getEmptyRecord();
			cout << "[Key: " << getKey(leafRecord)->toString() << "] ";
		}
		cout << endl;
	}
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}


#endif
