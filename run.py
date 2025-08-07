import argparse
import carla
import pygame
from src.world import World
from src.control_stack.hud import HUD
from src.control_stack.keyboard_control import KeyboardControl

def game_loop(args):
    """
    Main game loop for the simulation.
    """
    pygame.init()
    pygame.font.init()
    world = None

    try:
        client = carla.Client(args.host, args.port)
        client.set_timeout(10.0)
        
        display = pygame.display.set_mode(
            (args.width, args.height),
            pygame.HWSURFACE | pygame.DOUBLEBUF)
        
        hud = HUD(args.width, args.height)
        sim_world = client.load_world(args.town)
        world = World(sim_world, hud, args)
        controller = KeyboardControl(world, args.autopilot)

        clock = pygame.time.Clock()
        while True:
            clock.tick_busy_loop(60)
            if controller.parse_events(client, world, clock, False):
                return
            world.tick(clock)
            world.render(display)
            pygame.display.flip()

    finally:
        if world is not None:
            world.destroy()
        pygame.quit()

def main():
    argparser = argparse.ArgumentParser(description="E2E Autonomous Driving Simulation")
    argparser.add_argument('--host', default='127.0.0.1')
    argparser.add_argument('--port', default=2000, type=int)
    argparser.add_argument('--width', default=1280, type=int)
    argparser.add_argument('--height', default=720, type=int)
    argparser.add_argument('--town', default='Town05')
    argparser.add_argument('--autopilot', action='store_true')
    argparser.add_argument('--model', default='vehicle.tesla.model3')
    args = argparser.parse_args()

    game_loop(args)

if __name__ == '__main__':
    main() 