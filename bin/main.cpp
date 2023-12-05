#include "lib/DB_controller.h"

int main() {
    DB::MyAwesomeDB db;
    DB::Controller controller(db);
    std::string input;
    std::string line;
    bool flag = false;
    std::cout << "-- ENTER \"STOP\" TO STOP THE PROGRAM --\n" << std::endl;
    while (true) {
        while (input.find(';') >= input.size()) {
            std::getline(std::cin, line);
            if (line == "STOP") {
                flag = true;
                break;
            }
            input += line;
        }
        if (flag)
            break;
        controller.ReadInput(input);
        input = "";
    }

    return 0;
}
