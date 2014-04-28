class Executor(object):
  def registered(self, driver, executor_info, framework_info, slave_info):
    pass

  def reregistered(self, driver, slave_info):
    pass

  def disconnected(self, driver):
    pass

  def launch_task(self, driver, task):
    pass

  def kill_task(self, driver, task_id):
    pass

  def framework_message(self, data):
    pass

  def shutdown(self, driver):
    pass

  def error(self, driver, message):
    pass

  launchTask = launch_task
  killTask = kill_task
  frameworkMessage = framework_message


class ExecutorDriver(object):
  def start(self):
    pass

  def stop(self):
    pass

  def abort(self):
    pass

  def join(self):
    pass

  def run(self):
    pass

  def send_status_update(self, status):
    pass

  def send_framework_message(self, data):
    pass

  sendStatusUpdate = send_status_update
  sendFrameworkMessage = send_framework_message


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
