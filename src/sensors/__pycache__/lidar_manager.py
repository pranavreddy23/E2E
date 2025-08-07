import carla
import numpy as np
import pygame
import weakref

class LidarManager:
    def __init__(self, parent_actor, hud):
        self.sensor = None
        self.surface = None
        self._parent = parent_actor
        self._hud = hud
        self.lidar_range = 80.0
        
        world = self._parent.get_world()
        bp_library = world.get_blueprint_library()
        bp = bp_library.find('sensor.lidar.ray_cast')
        bp.set_attribute('range', str(self.lidar_range))
        
        self.sensor = world.spawn_actor(
            bp,
            carla.Transform(carla.Location(z=2.4)),
            attach_to=self._parent)
            
        weak_self = weakref.ref(self)
        self.sensor.listen(lambda data: LidarManager._parse_lidar(weak_self, data))

    def render(self, display):
        if self.surface is not None:
            display.blit(self.surface, (0, 0))

    @staticmethod
    def _parse_lidar(weak_self, data):
        self = weak_self()
        if not self:
            return

        points = np.frombuffer(data.raw_data, dtype=np.dtype('f4'))
        points = np.reshape(points, (int(points.shape[0] / 4), 4))
        lidar_data = np.array(points[:, :2])
        lidar_data *= min(self._hud.width, self._hud.height) / (2.0 * self.lidar_range)
        lidar_data += (0.5 * self._hud.width, 0.5 * self._hud.height)
        lidar_data = lidar_data.astype(np.int32)
        
        lidar_img = np.zeros((self._hud.width, self._hud.height, 3), dtype=np.uint8)
        valid_points = (lidar_data[:, 0] >= 0) & (lidar_data[:, 0] < self._hud.width) & \
                       (lidar_data[:, 1] >= 0) & (lidar_data[:, 1] < self._hud.height)
        lidar_data = lidar_data[valid_points]
        lidar_img[lidar_data[:, 1], lidar_data[:, 0]] = (255, 255, 255)
        
        self.surface = pygame.surfarray.make_surface(lidar_img.swapaxes(0, 1))

    def destroy(self):
        if self.sensor:
            self.sensor.destroy() 