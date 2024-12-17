// Cloudless.cpp: определяет точку входа для приложения.
//


#include "CachedFileIOTest.h"

#include "Cloudless.h"

using namespace std;

int main()
{
	const char* path = "D:\\cachedfile.db";
	Cloudless::CachedFileIOTest cfiot(path);

	cfiot.run();
	
	return 0;
}
