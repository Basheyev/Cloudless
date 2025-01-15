
#include "CloudlessTests.h"
#include "CachedFileIO.h"
#include "RecordFileIO.h"
#include "TestCachedFileIO.h"
#include "TestRecordFileIO.h"

#include <ctime>
#include <iomanip>
#include <iostream>

using namespace std;
using namespace Cloudless;
using namespace Cloudless::Storage;
using namespace Cloudless::Tests;


//-----------------------------------------------------------------------------
void CloudlessTests::run()
{
	for (ITestCase* tc : testCases) {		
		std::cout << "\n[TEST] " << tc->getName() << ":\n";
		tc->init();
		tc->execute();
		bool result = tc->verify();		
		tc->cleanup();
	}
}

//-----------------------------------------------------------------------------

int main()
{
	CloudlessTests ct;	
	TestCachedFileIO cfiot;
	TestRecordFileIO rfiot;

	//ct.addTestCase(&cfiot);
	ct.addTestCase(&rfiot);

	std::filesystem::current_path("F:/");

	ct.run();
		
	return 0;
}