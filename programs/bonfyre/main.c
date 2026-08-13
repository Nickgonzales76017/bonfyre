#include "bonfyre_fabric.h"

#include <stdio.h>

int main(int argc, char **argv) {
    if (argc < 2) return bf_fabric_dispatch(1, (char *[]){"help"}, stdout, stderr);
    return bf_fabric_dispatch(argc - 1, argv + 1, stdout, stderr);
}
