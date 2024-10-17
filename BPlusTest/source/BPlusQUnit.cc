
#ifndef BPLUS_TEST_H
#define BPLUS_TEST_H

#include "MyDB_AttType.h"  
#include "MyDB_BufferManager.h"
#include "MyDB_Catalog.h"  
#include "MyDB_Page.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_Record.h"
#include "MyDB_Table.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_Schema.h"
#include "QUnit.h"
#include "Sorting.h"
#include <iostream>

#define FALLTHROUGH_INTENDED do {} while (0)

int main (int argc, char *argv[]) {

	int start = 1;
	if (argc > 1 && argv[1][0] >= '0' && argv[1][0] <= '9') {
		start = argv[1][0] - '0';
	}

	QUnit::UnitTest qunit(cerr, QUnit::normal);

	// create a catalog
	MyDB_CatalogPtr myCatalog = make_shared <MyDB_Catalog> ("catFile");

	// now make a schema
	MyDB_SchemaPtr mySchema = make_shared <MyDB_Schema> ();
	mySchema->appendAtt (make_pair ("suppkey", make_shared <MyDB_IntAttType> ()));
	mySchema->appendAtt (make_pair ("name", make_shared <MyDB_StringAttType> ()));
	mySchema->appendAtt (make_pair ("address", make_shared <MyDB_StringAttType> ()));
	mySchema->appendAtt (make_pair ("nationkey", make_shared <MyDB_IntAttType> ()));
	mySchema->appendAtt (make_pair ("phone", make_shared <MyDB_StringAttType> ()));
	mySchema->appendAtt (make_pair ("acctbal", make_shared <MyDB_DoubleAttType> ()));
	mySchema->appendAtt (make_pair ("comment", make_shared <MyDB_StringAttType> ()));

	// use the schema to create a table
	MyDB_TablePtr myTable = make_shared <MyDB_Table> ("supplier", "supplier.bin", mySchema);

	cout << "Using small page size.\n";

	switch (start) {
	case 1:
	{
		cout << "TEST 4... creating tree for large table, on comment " << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("comment", myTable, myMgr);
		supplierTable.loadFromTextFile ("supplierBig.tbl");

		// there should be 320000 records
		MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
		MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

		int counter = 0;
		while (myIter->advance ()) {
				myIter->getCurrent (temp);
				counter++;
		}
		bool result = (counter == 320000);
		if (result)
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
                QUNIT_IS_TRUE (result);
	}
	}
}

#endif
