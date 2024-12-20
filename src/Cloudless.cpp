// Cloudless.cpp: определяет точку входа для приложения.
//

#include <shared_mutex>

#include "CachedFileIOTest.h"
#include "RecordFileIOTest.h"

#include "Cloudless.h"

using namespace std;


int main()
{
	const char* path = "D:\\cachedfile.db";
	Cloudless::CachedFileIOTest cfiot(path);
	cfiot.run();

//	Cloudless::RecordFileIOTest tst;
//	tst.run(path);

//	std::cout << "Cache page size: " << sizeof(Cloudless::CachePage) << "\n";

	return 0;
}
