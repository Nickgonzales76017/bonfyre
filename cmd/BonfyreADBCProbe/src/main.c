// Probe the installed ADBC Flight SQL driver's C ABI without opening a DB or
// contacting a server. A successful probe is not an executed SQL query.
#include <adbc.h>
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>

static int fail(const char *message) {
  fprintf(stderr, "adbc-probe: %s\n", message);
  return 1;
}

int main(int argc, char **argv) {
  if (argc != 2 || !argv[1][0]) return fail("usage: bonfyre-adbc-probe DRIVER_LIBRARY");
  void *library = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
  if (!library) return fail(dlerror());
  AdbcDriverInitFunc init = (AdbcDriverInitFunc)dlsym(library, "AdbcDriverInit");
  if (!init) {
    dlclose(library);
    return fail("driver does not export AdbcDriverInit");
  }
  struct AdbcDriver driver = {0};
  struct AdbcError error = {0};
  int version = ADBC_VERSION_1_1_0;
  AdbcStatusCode status = init(version, &driver, &error);
  if (status == ADBC_STATUS_NOT_IMPLEMENTED) {
    if (error.release) error.release(&error);
    memset(&error, 0, sizeof(error));
    memset(&driver, 0, sizeof(driver));
    version = ADBC_VERSION_1_0_0;
    status = init(version, &driver, &error);
  }
  if (status != ADBC_STATUS_OK || !driver.DatabaseNew ||
      !driver.ConnectionNew || !driver.StatementNew || !driver.StatementExecuteQuery) {
    fprintf(stderr, "adbc-probe: init status=%d release=%d database=%d connection=%d statement=%d execute=%d\n",
            (int)status, driver.release != NULL, driver.DatabaseNew != NULL,
            driver.ConnectionNew != NULL, driver.StatementNew != NULL,
            driver.StatementExecuteQuery != NULL);
    if (error.message) fprintf(stderr, "adbc-probe: %s\n", error.message);
    if (error.release) error.release(&error);
    if (driver.release) driver.release(&driver, NULL);
    dlclose(library);
    return fail("driver initialization or required ABI table failed");
  }
  if (error.release) error.release(&error);
  struct AdbcError release_error = {0};
  status = driver.release ? driver.release(&driver, &release_error) : ADBC_STATUS_OK;
  if (status != ADBC_STATUS_OK) {
    if (release_error.message) fprintf(stderr, "adbc-probe: %s\n", release_error.message);
    if (release_error.release) release_error.release(&release_error);
    dlclose(library);
    return fail("driver release failed");
  }
  if (release_error.release) release_error.release(&release_error);
  dlclose(library);
  printf("{\"state\":\"driver_initialized_unserved\",\"abi_version\":%d}\n", version);
  return 0;
}
