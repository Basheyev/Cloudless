// Cloudless.cpp: определяет точку входа для приложения.
//


#include "Cloudless.h"

#include "Windows.h"

using namespace std;
using namespace Cloudless::Storage;


// Вспомогательная функция для получения пути к браузеру по ProgId
std::string GetBrowserPathFromProgId(const std::string& progId) {
    HKEY hKey;
    char path[MAX_PATH];
    DWORD pathSize = sizeof(path);
    std::string keyPath = "SOFTWARE\\Classes\\" + progId + "\\shell\\open\\command";

    if (RegOpenKeyEx(HKEY_LOCAL_MACHINE, keyPath.c_str(), 0, KEY_READ, &hKey) == ERROR_SUCCESS ||
        RegOpenKeyEx(HKEY_CURRENT_USER, keyPath.c_str(), 0, KEY_READ, &hKey) == ERROR_SUCCESS) {

        if (RegQueryValueEx(hKey, NULL, NULL, NULL, (LPBYTE)path, &pathSize) == ERROR_SUCCESS) {
            RegCloseKey(hKey);

            std::string browserPath = path;
            size_t firstQuote = browserPath.find('\"');
            size_t secondQuote = browserPath.find('\"', firstQuote + 1);

            if (firstQuote != std::string::npos && secondQuote != std::string::npos) {
                browserPath = browserPath.substr(firstQuote + 1, secondQuote - firstQuote - 1);
            }

            return browserPath;
        }
        RegCloseKey(hKey);
    }

    return "";
}

// Основная функция для определения браузера по умолчанию
std::string GetDefaultBrowserPath() {
    HKEY hKey;
    char progId[256];
    DWORD progIdSize = sizeof(progId);

    // Открытие ключа UserChoice для протокола HTTP
    if (RegOpenKeyEx(HKEY_CURRENT_USER,
        "Software\\Microsoft\\Windows\\Shell\\Associations\\UrlAssociations\\http\\UserChoice",
        0, KEY_READ, &hKey) != ERROR_SUCCESS) {
        return "";
    }

    if (RegQueryValueEx(hKey, "ProgId", NULL, NULL, (LPBYTE)progId, &progIdSize) != ERROR_SUCCESS) {
        RegCloseKey(hKey);
        return "";
    }

    RegCloseKey(hKey);

    // Получение пути к браузеру через ProgId
    return GetBrowserPathFromProgId(progId);
}


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

        std::string defaultBrowserPath = GetDefaultBrowserPath();
        std::cout << "Default browser: " << defaultBrowserPath << "\n";
        ShellExecute(NULL, "open", defaultBrowserPath.c_str(), "-app=http://localhost:8080", NULL, SW_SHOW);

        // Keep the server running
        std::cout << "Press Enter to stop the server..." << std::endl;
        std::cin.get();

    }
    catch (const std::exception& e) {
        std::cerr << "Failed to start the server: " << e.what() << std::endl;
    }

    return 0;
}
