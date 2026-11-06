CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-resource-weather-refresh

.PHONY: all test clean
all: $(TARGET) test
$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $<
test: $(TARGET)
	$(MAKE) -C ../BonfyreMaterialLease bonfyre-material-lease
	$(MAKE) -C ../BonfyreMaterialWeatherGate bonfyre-material-weather-gate
	$(MAKE) -C ../BonfyreResourceWeatherDelta bonfyre-resource-weather-delta
	$(MAKE) -C ../BonfyreMaterialWeatherReheat bonfyre-material-weather-reheat
	$(MAKE) -C ../BonfyreResourceWeatherTransition bonfyre-resource-weather-transition
	$(MAKE) -C ../../components/BonfyreProject bonfyre-project
	sh tests/resource_weather_refresh_test.sh ./$(TARGET) ../../components/BonfyreProject/bonfyre-project ../BonfyreResourceWeatherTransition/bonfyre-resource-weather-transition ../BonfyreMaterialLease/bonfyre-material-lease ../BonfyreMaterialWeatherGate/bonfyre-material-weather-gate ../BonfyreResourceWeatherDelta/bonfyre-resource-weather-delta ../BonfyreMaterialWeatherReheat/bonfyre-material-weather-reheat
clean:
	rm -f $(TARGET)
