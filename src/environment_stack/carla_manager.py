import carla
import yaml
import random

class CarlaManager:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.client = carla.Client('localhost', 2000)
        self.client.set_timeout(10.0)
        self.world = self.client.load_world(self.config['town'])
        self.ego_vehicle = None
        self.setup_world()

    def setup_world(self):
        self.set_weather()
        self.spawn_actors()

    def set_weather(self):
        weather_params = carla.WeatherParameters(
            cloudiness=self.config['weather']['cloudiness'],
            precipitation=self.config['weather']['precipitation'],
            precipitation_deposits=self.config['weather']['precipitation_deposits'],
            wind_intensity=self.config['weather']['wind_intensity'],
            sun_altitude_angle=self.config['weather']['sun_altitude_angle'],
            fog_density=self.config['weather']['fog_density']
        )
        self.world.set_weather(weather_params)

    def spawn_actors(self):
        blueprint_library = self.world.get_blueprint_library()
        
        # Spawn ego vehicle
        ego_config = self.config['ego_vehicle']
        ego_bp = blueprint_library.find(ego_config['model'])
        spawn_points = self.world.get_map().get_spawn_points()
        spawn_point = random.choice(spawn_points) if spawn_points else carla.Transform()
        self.ego_vehicle = self.world.spawn_actor(ego_bp, spawn_point)

    def close(self):
        if self.ego_vehicle:
            self.ego_vehicle.destroy()

if __name__ == '__main__':
    try:
        manager = CarlaManager('configs/environment/environment.yaml')
        # Keep the script running
        while True:
            manager.world.wait_for_tick()
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        if manager:
            manager.close()
            print("Simulation cleaned up.") 