import argparse
from src.environment_stack.carla_manager import CarlaManager
from src.control_stack.hud import HUD
from src.sensors.camera_manager import CameraManager
from src.sensors.lidar_manager import LidarManager
from src.control_stack.keyboard_control import KeyboardControl
import pygame

def game_loop(args):
    """
    Main game loop for the simulation.
    """
    pygame.init()
    pygame.font.init()
    manager = None
    sensor_managers = []
    try:
        manager = CarlaManager(args.config)
        
        hud = HUD(1280, 720)
        
        # Initialize sensor managers
        camera_manager = CameraManager(manager.ego_vehicle, hud)
        lidar_manager = LidarManager(manager.ego_vehicle, hud)
        sensor_managers = [camera_manager, lidar_manager]

        controller = KeyboardControl(manager.ego_vehicle, sensor_managers)
        
        clock = pygame.time.Clock()
        while True:
            clock.tick_busy_loop(60)
            if controller.parse_events():
                return

            manager.world.tick()
            
            active_sensor = controller.get_active_sensor_manager()
            hud.render(active_sensor)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        for sensor in sensor_managers:
            sensor.destroy()
        if manager:
            manager.close()
        pygame.quit()

def main(args):
    """
    Main entry point for the E2E autonomous driving simulation.
    """
    game_loop(args)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="E2E Autonomous Driving Simulation")
    parser.add_argument('--config', type=str, default='configs/environment/environment.yaml',
                        help='Path to the environment configuration file.')
    args = parser.parse_args()
    main(args) 