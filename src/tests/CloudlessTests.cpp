
#include "CloudlessTests.h"
#include "CachedFileIO.h"
#include "RecordFileIO.h"
#include "TestCachedFileIO.h"

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
		std::cout << "[TEST] " << tc->getName() << ":\n";
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

	ct.addTestCase(&cfiot);

	ct.run();
		
	return 0;
}