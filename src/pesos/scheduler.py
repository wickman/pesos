from .vendor import mesos

from compactor.process import ProtobufProcess


class SchedulerProcess(ProtobufProcess):

  @ProtobufProcess.install(mesos.internal.FrameworkRegisteredMessage)
  def registered(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.FrameworkReregisteredMessage)
  def reregistered(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.ResourceOffersMessage)
  def resource_offers(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.RescindResourceOfferMessage)
  def rescind_offer(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.StatusUpdateMessage)
  def status_update(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.LostSlaveMessage)
  def lost_slave(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.ExecutorToFrameworkMessage)
  def framework_message(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.FrameworkErrorMessage)
  def error(self, from_pid, message):
    pass



class Scheduler(object):
  def registered(self, driver, framework_id, master_info):
    pass

  def reregistered(self, driver, master_info):
    pass

  def disconnected(self, driver):
    pass

  def resource_offers(self, driver, offers):
    pass

  def offer_rescinded(self, driver, offer_id):
    pass

  def status_update(self, driver, status):
    pass

  def framework_message(self, driver, executor_id, slave_id, data):
    pass

  def slave_lost(self, driver, slave_id):
    pass

  def executor_lost(self, driver, executor_id, slave_id, status):
    pass

  def error(self, driver, message):
    pass


class SchedulerDriver(object):
  def start(self):
    pass

  def stop(self, failover=False):
    pass

  def abort(self):
    pass

  def join(self):
    pass

  def run(self):
    pass

  def request_resources(self, requests):
    pass

  def launch_tasks(self, offer_ids, tasks, filters=None):
    pass

  def kill_task(self, task_id):
    pass

  def decline_offer(self, offer_id, filters=None):
    pass

  def revive_offers(self):
    pass

  def send_framework_message(self, executor_id, slave_id, data):
    pass

  def reconcile_tasks(self, statuses):
    pass


class MesosSchedulerDriver(SchedulerDriver):
  def __init__(self, scheduler, framework, master, credential=None):
    self.scheduler = scheduler
    self.scheduler_process, self.scheduler_pid = None, None
    self.master = master
    self.framework = framework
    self.lock = threading.Condition()
    self.credential = credential
    self.detector = None
