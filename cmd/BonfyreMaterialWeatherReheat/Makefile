CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-material-weather-reheat
.PHONY: all test clean
all: $(TARGET) test
$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3
test: $(TARGET)
	$(MAKE) -C ../BonfyreMaterialLease bonfyre-material-lease
	$(MAKE) -C ../BonfyreMaterialWeatherGate bonfyre-material-weather-gate
	$(MAKE) -C ../BonfyreResourceWeatherDelta bonfyre-resource-weather-delta
	$(MAKE) -C ../BonfyreLoweringWorkers bonfyre-lowering-workers
	$(MAKE) -C ../BonfyreWebSub bonfyre-websub
	$(MAKE) -C ../BonfyreExternalEstate bonfyre-external-estate
	sh tests/material_weather_reheat_test.sh ./$(TARGET) ../BonfyreMaterialLease/bonfyre-material-lease ../BonfyreMaterialWeatherGate/bonfyre-material-weather-gate ../BonfyreResourceWeatherDelta/bonfyre-resource-weather-delta
	sh tests/material_weather_reheat_scale_test.sh ./$(TARGET) ../BonfyreMaterialLease/bonfyre-material-lease ../BonfyreMaterialWeatherGate/bonfyre-material-weather-gate ../BonfyreResourceWeatherDelta/bonfyre-resource-weather-delta
	sh tests/weather_material_bridge_test.sh ./$(TARGET) ../BonfyreMaterialLease/bonfyre-material-lease ../BonfyreMaterialWeatherGate/bonfyre-material-weather-gate ../BonfyreResourceWeatherDelta/bonfyre-resource-weather-delta ../BonfyreLoweringWorkers/bonfyre-lowering-workers ../BonfyreWebSub/bonfyre-websub ../BonfyreExternalEstate/bonfyre-external-estate
clean:
	rm -f $(TARGET)
