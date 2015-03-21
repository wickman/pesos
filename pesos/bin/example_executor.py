import getpass
import logging
import sys
import uuid

from pesos.executor import PesosExecutorDriver
from pesos.vendor.mesos import mesos_pb2

from compactor.context import Context
from mesos.interface import Executor


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger()


class ExampleExecutor(Executor):
  def registered(self, driver, executor_info, framework_info, slave_info):
    log.info('Registered:')
    log.info('  driver: %r' % driver)
    log.info('  executor_info: %r' % executor_info)
    log.info('  framework_info: %r' % framework_info)
    log.info('  slave_info: %r' % slave_info)

  def launch_task(self, driver, task):
    log.info('Got launch_task:')
    log.info('  task: %r' % task)

    status = mesos_pb2.TaskStatus()
    status.task_id.MergeFrom(task.task_id)
    status.state = mesos_pb2.TASK_FINISHED

    driver.send_status_update(status)

  def shutdown(self, driver):
    log.info('Told to shutdown.')
    driver.stop()
    log.info('Finished shutdown.')


def main(args):
  context = Context.singleton()
  executor = ExampleExecutor()
  driver = PesosExecutorDriver(executor, context=context)

  print('Starting driver')
  driver.start()

  print('Joining driver')
  driver.join()

  context.stop()


if __name__ == '__main__':
  main(sys.argv[1:])
