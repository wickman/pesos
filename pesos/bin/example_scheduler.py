import getpass
import logging
import sys
import uuid

from pesos.scheduler import PesosSchedulerDriver
from pesos.vendor.mesos import mesos_pb2

from mesos.interface import Scheduler


logging.basicConfig(level=logging.DEBUG)


class Resources(object):
  __slots__ = ('cpu', 'ram', 'disk')

  SCALAR_MAPPING = {
    'cpu': 'cpus',
    'ram': 'mem',
    'disk': 'disk',
  }

  @classmethod
  def sum(cls, resource_iterable, role='*'):
    cpu, ram, disk = 0, 0, 0

    for resource in resource_iterable:
      if resource.role != role:
        continue
      if resource.name == 'cpus':
        cpu += resource.scalar.value
      elif resource.name == 'mem':
        ram += resource.scalar.value
      elif resource.name == 'disk':
        disk += resource.scalar.value

    return cls(cpu, ram, disk)

  @classmethod
  def from_proto(cls, proto, role='*'):
    return cls.sum(proto.resources, role=role)

  def __init__(self, cpu, ram, disk):
    self.cpu, self.ram, self.disk = cpu, ram, disk

  def _validate(self, other):
    if not isinstance(other, Resources):
      raise TypeError('Operator expected Resources, got %s' % type(other))

  def __add__(self, other):
    self._validate(other)
    return Resources(self.cpu + other.cpu, self.ram + other.ram, self.disk + other.disk)

  def __sub__(self, other):
    self._validate(other)
    return Resources(self.cpu - other.cpu, self.ram - other.ram, self.disk - other.disk)

  def __contains__(self, other):
    self._validate(other)
    return other.cpu <= self.cpu and other.ram <= self.ram and other.disk <= self.disk

  def write(self, proto):
    for source_attr, dest_attr in self.SCALAR_MAPPING.items():
      attr = proto.resources.add()
      attr.name = dest_attr
      attr.type = mesos_pb2.Value.SCALAR
      attr.scalar.value = getattr(self, source_attr)


class Task(object):
  def __init__(self, name, resources, execution_info):
    self.name = name
    self.resources = resources
    if not isinstance(execution_info, (Command, Executor)):
      raise TypeError('execution_info should be one of Command or Executor, got %s'
          % type(execution_info))
    self.execution_info = execution_info

  def write(self, proto):
    proto.name = self.name
    self.resources.write(proto)
    if isinstance(self.execution_info, Executor):
      self.execution_info.write(proto.executor)
    else:
      self.execution_info.write(proto.command)

class Executor(object):
  # required executor_id
  # required framework_id
  # required command
  # required resources
  # optional container_info (?)
  # optional name (?)
  # optional source
  # optional data
  def __init__(self, command, resources, source=None, data=None):
    self.command = command
    self.resources = resources
    self.source = source
    self.data = data

  def write(self, proto):
    self.command.write(proto.command)
    self.resources.write(proto)
    if self.source:
      proto.source = self.source
    if self.data:
      proto.data = self.data


class URI(object):
  __slots__ = ('uri', 'executable', 'extract')

  @classmethod
  def wrap(cls, value):
    if isinstance(value, cls):
      return value
    else:
      return cls(value)

  def __init__(self, uri, executable=False, extract=True):
    self.uri, self.executable, self.extract = uri, executable, extract

  def write(self, proto):
    proto.value = self.uri
    proto.executable = self.executable
    proto.extract = self.extract


def command_to_proto(
    proto,
    command,
    shell=True,
    user=None,
    env=None,
    uris=None):

  proto.shell = shell

  if isinstance(command, str):
    if not shell:
      raise ValueError('If not a shell, command must be a sequence of arguments.')
    proto.value = command
  else:
    proto.value = command[0]
    proto.arguments.extend(command[1:])

  if user:
    proto.user.value = user

  if env:
    environment = mesos_pb2.Environment()
    for key, val in env.items():
      variable = environment.variables.add()
      variable.name = key
      variable.value = val

  if uris:
    for uri in uris:
      URI.wrap(uri).write(proto.uris.add())

  return proto


class Command(object):
  # optional container
  # repeated URIs { value, executable, extract }
  # optional environment
  # shell=True
  # required value
  # optional arguments
  # optional user (user to launch, if not specified, inherits from framework)
  def __init__(self,
               command,
               shell=True,
               user=None,
               env=None,
               uris=None):
    self.command, self.shell, self.user, self.env, self.uris = (
        command, shell, user, env, uris)

  def write(self, proto):
    command_to_proto(
        proto,
        self.command,
        self.shell,
        self.user,
        self.env,
        self.uris)


class ExampleScheduler(Scheduler):
  def __init__(self):
    self._pending_queue = []

  def registered(self, driver, framework_id, master_info):
    print('registered: framework_id: %s, master_info: %s' % (framework_id, master_info))

  def add_pending(self, task):
    self._pending_queue.append(task)

  @classmethod
  def _first_fit(cls, offer_map, task):
    for offer_id, offer_info in offer_map.items():
      offer, resources = offer_info
      if task.resources in resources:
        print('Resource matched: %r to %s' % (task, offer))
        return offer_id
      else:
        print('Resource insufficient: %r to %s' % (task, offer))

  def _pending_to_scheduled(self, schedule_queue):
    scheduled_tasks = []
    for offer, task in schedule_queue:
      task_info = mesos_pb2.TaskInfo()
      task.write(task_info)

      # populate task_id, slave_id
      task_info.task_id.value = '%s-%s' % (task.name, uuid.uuid4())
      task_info.slave_id.MergeFrom(offer.slave_id)

      scheduled_tasks.append(task_info)
    return scheduled_tasks

  def resource_offers(self, driver, offers):
    all_offers = {}

    for offer in offers:
      all_offers[offer.id.value] = (offer, Resources.sum(offer.resources))

    pendingq = []
    scheduleq = []

    for task in self._pending_queue:
      offer_id = self._first_fit(all_offers, task)
      if offer_id:
        # debit resources from offer map
        offer, resources = all_offers[offer_id]
        all_offers[offer_id] = (offer, resources - task.resources)
        scheduleq.append((offer, task))
      else:
        pendingq.append(task)

    driver.launch_tasks(all_offers, self._pending_to_scheduled(scheduleq))
    self._pending_queue = pendingq

  def status_update(self, driver, status):
    print('Got status update: %s' % status)


def main(args):
  scheduler = ExampleScheduler()
  framework = mesos_pb2.FrameworkInfo(
      user='vagrant',
      name='example',
  )
  driver = PesosSchedulerDriver(
      scheduler=scheduler,
      framework=framework,
      master_uri=args[0],
  )

  task = Task(
      'hello_world',
      Resources(1, 256, 256),
      Command('echo hello world'),
  )

  scheduler.add_pending(task)

  print('Starting driver')
  driver.start()

  print('Joining driver')
  driver.join()


if __name__ == '__main__':
  main(sys.argv[1:])
