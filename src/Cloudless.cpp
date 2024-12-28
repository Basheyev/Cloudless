// Cloudless.cpp: определяет точку входа для приложения.
//


#include "Cloudless.h"


using namespace std;
using namespace Cloudless::Storage;



int main()
{
    try {
        // Get the current working directory        
        std::string appDirectory = std::filesystem::canonical("navigator").string();;
        

        // Set the options for the CivetWeb server
        const char* options[] = {
            "document_root", appDirectory.c_str(), // Serve files from app directory
            "listening_ports", "8080",                   // Port to listen on
            "index_files", "index.html",                 // Использовать index.html как стартовый файл
            nullptr                                      // End of options
        };

        std::cout << std::filesystem::exists(appDirectory);

        // Initialize CivetWeb server
        CivetServer server(options);

        std::cout << "Server started on http://localhost:8080" << std::endl;
        std::cout << "Serving files from: " << appDirectory << std::endl;

        // Keep the server running
        std::cout << "Press Enter to stop the server..." << std::endl;
        std::cin.get();

    }
    catch (const std::exception& e) {
        std::cerr << "Failed to start the server: " << e.what() << std::endl;
    }

    return 0;
}
