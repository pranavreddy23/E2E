import carla
import pygame
from pygame.locals import K_w, K_s, K_a, K_d, K_q, K_SPACE, K_ESCAPE, K_TAB

class KeyboardControl:
    def __init__(self, vehicle, sensor_managers):
        self._vehicle = vehicle
        self._sensor_managers = sensor_managers
        self._sensor_index = 0
        self._control = carla.VehicleControl()
        self._steer_cache = 0.0

    def get_active_sensor_manager(self):
        return self._sensor_managers[self._sensor_index]

    def parse_events(self):
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                return True
            elif event.type == pygame.KEYUP:
                if event.key == K_ESCAPE:
                    return True
                elif event.key == K_TAB:
                    self._sensor_index = (self._sensor_index + 1) % len(self._sensor_managers)
                elif event.key == K_q:
                    self._control.reverse = not self._control.reverse
        
        self._parse_vehicle_keys(pygame.key.get_pressed())
        self._control.reverse = self._control.gear < 0
        self._vehicle.apply_control(self._control)
        return False

    def _parse_vehicle_keys(self, keys):
        if keys[K_w]:
            self._control.throttle = min(self._control.throttle + 0.1, 1.0)
        else:
            self._control.throttle = 0.0

        if keys[K_s]:
            self._control.brake = min(self._control.brake + 0.2, 1)
        else:
            self._control.brake = 0

        steer_increment = 0.05
        if keys[K_a]:
            self._steer_cache -= steer_increment
        elif keys[K_d]:
            self._steer_cache += steer_increment
        else:
            self._steer_cache = 0.0
        
        self._steer_cache = min(0.7, max(-0.7, self._steer_cache))
        self._control.steer = round(self._steer_cache, 1)
        self._control.hand_brake = keys[K_SPACE] 