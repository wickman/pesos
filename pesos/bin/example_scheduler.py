import getpass
import logging
import sys

from pesos.api import Scheduler
from pesos.scheduler import MesosSchedulerDriver
from pesos.vendor.mesos import FrameworkInfo


logging.basicConfig(level=logging.DEBUG)


class ExampleScheduler(Scheduler):
  def registered(self, driver, framework_id, master_info):
    print('registered: framework_id: %s, master_info: %s' % (framework_id, master_info))

  def resource_offers(self, driver, offers):
    for offer in offers:
      print('offer: %s' % offer)


def main(args):
  scheduler = ExampleScheduler()
  framework = FrameworkInfo(
      user=getpass.getuser(),
      name='example',
  )
  driver = MesosSchedulerDriver(
      scheduler=scheduler,
      framework=framework,
      master_uri=args[0],
  )

  print('Starting driver')
  driver.start()

  print('Joining driver')
  driver.join()


if __name__ == '__main__':
  main(sys.argv[1:])
