CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-resource-weather-transition

.PHONY: all test clean
all: $(TARGET) test
$(TARGET): src/main.c ../../lib/libbonfyre/src/bf_sha256.c
	$(CC) $(CFLAGS) -I../../lib/libbonfyre/include -o $@ $< ../../lib/libbonfyre/src/bf_sha256.c -lsqlite3
test: $(TARGET)
	$(MAKE) -C ../BonfyreMaterialLease bonfyre-material-lease
	$(MAKE) -C ../BonfyreMaterialWeatherGate bonfyre-material-weather-gate
	$(MAKE) -C ../BonfyreResourceWeatherDelta bonfyre-resource-weather-delta
	$(MAKE) -C ../BonfyreMaterialWeatherReheat bonfyre-material-weather-reheat
	$(MAKE) -C ../../components/BonfyreProject bonfyre-project
	sh tests/resource_weather_transition_test.sh ./$(TARGET) ../BonfyreMaterialLease/bonfyre-material-lease ../BonfyreMaterialWeatherGate/bonfyre-material-weather-gate ../BonfyreResourceWeatherDelta/bonfyre-resource-weather-delta ../BonfyreMaterialWeatherReheat/bonfyre-material-weather-reheat
	sh tests/project_weather_transition_test.sh ./$(TARGET) ../BonfyreMaterialLease/bonfyre-material-lease ../BonfyreMaterialWeatherGate/bonfyre-material-weather-gate ../BonfyreResourceWeatherDelta/bonfyre-resource-weather-delta ../BonfyreMaterialWeatherReheat/bonfyre-material-weather-reheat ../../components/BonfyreProject/bonfyre-project
clean:
	rm -f $(TARGET)
