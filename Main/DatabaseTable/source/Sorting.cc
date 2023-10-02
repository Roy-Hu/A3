
#ifndef SORT_C
#define SORT_C

#include "algorithm"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "RecordComparator.h"
#include "Sorting.h"

using namespace std;

vector <MyDB_PageReaderWriter> mergeSort (MyDB_BufferManagerPtr parent, vector <MyDB_PageReaderWriterPtr> sortMe, int low, int high,
        function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	int mid = (low + high)  / 2;
	vector <MyDB_PageReaderWriter> lSortedList, rSortedList, mergedList;
	
	if (high > low) {
		MyDB_RecordIteratorAltPtr lSortedListIter, rSortedListIter;

		lSortedList = mergeSort(parent, sortMe, low, mid, comparator, lhs, rhs);
		rSortedList = mergeSort(parent, sortMe, mid + 1, high, comparator, lhs, rhs);
		lSortedListIter = getIteratorAlt(lSortedList);
		rSortedListIter = getIteratorAlt(rSortedList);

		mergedList = mergeIntoList(parent, lSortedListIter, rSortedListIter, comparator, lhs, rhs);
	} else if (high == low) {
		mergedList.push_back(*(sortMe[low]));
	}

	return mergedList;
}

void mergeIntoFile (MyDB_TableReaderWriter &sortIntoMe, vector <MyDB_RecordIteratorAltPtr> &mergeUs,
        function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	
	RecordInIterComparator myComparator (comparator, lhs, rhs);

	priority_queue <MyDB_RecordIteratorAltPtr, vector <MyDB_RecordIteratorAltPtr>, RecordInIterComparator> priorityQueue(myComparator);

	for (auto iter : mergeUs) {
		priorityQueue.push(iter);
	}

	while (priorityQueue.size() != 0) {
		MyDB_RecordIteratorAltPtr minRecIter = priorityQueue.top();
		minRecIter->getCurrent(lhs);
		sortIntoMe.append(lhs);

		priorityQueue.pop();

		if (minRecIter->advance()) {
			priorityQueue.push(minRecIter);
		}
	}
}

vector <MyDB_PageReaderWriter> mergeIntoList (MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter,
        MyDB_RecordIteratorAltPtr rightIter, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	
	vector <MyDB_PageReaderWriter> sortedPages;

	vector <void *> lRecordList, rRecordList;
	void *curRecord;
	while(leftIter->advance()) {
		curRecord = malloc(parent->getPageSize());
		leftIter->getCurrent(lhs);
		lhs->toBinary(curRecord);
		lRecordList.push_back(curRecord);
	}

	while(rightIter->advance()) {
		curRecord = malloc(parent->getPageSize());
		rightIter->getCurrent(lhs);
		lhs->toBinary(curRecord);
		rRecordList.push_back(curRecord);
	}

	vector <void *> mergedList;
	RecordComparator myComparator (comparator, lhs, rhs);
	merge(lRecordList.begin(), lRecordList.end(), rRecordList.begin(), rRecordList.end(), back_inserter(mergedList), myComparator);

	MyDB_PageReaderWriterPtr resultPage = make_shared <MyDB_PageReaderWriter> (*parent);

	for (auto rec : mergedList) {
		lhs->fromBinary(rec);
		if (!resultPage->append(lhs)) {
			sortedPages.push_back(*resultPage);
			resultPage = make_shared <MyDB_PageReaderWriter> (*parent);
			if (!resultPage->append(lhs)) {
				perror("fail to add a record after adding a new page");
				break;
			}
		}
		// In append it use toBunary, which use memcpy to copy data, so we can free the record here
		free(rec);
	}

	sortedPages.push_back(*resultPage);
	
	return sortedPages;
} 
	
void sort (int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe,
        function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	vector <MyDB_RecordIteratorAltPtr> mergedListIter;
	for (int i = 0; i < sortMe.getNumPages(); i += runSize) {
		vector <MyDB_PageReaderWriterPtr> unmergedRun;

		for (int j = 0; j < runSize && i + j < sortMe.getNumPages(); j++) {
			unmergedRun.push_back(sortMe[i + j].sort(comparator, lhs, rhs));
		}

		vector <MyDB_PageReaderWriter> mergedRun = mergeSort(sortMe.getBufferMgr(), unmergedRun, 0, unmergedRun.size() - 1, comparator, lhs, rhs);
		
		for (auto page : mergedRun) {
			mergedListIter.push_back(page.getIteratorAlt());
		}
	}

	mergeIntoFile(sortIntoMe, mergedListIter, comparator, lhs, rhs);
} 

#endif
