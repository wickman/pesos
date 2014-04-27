from .vendor import mesos

from compactor.process import ProtobufProcess 


class ExecutorProcessBase(ProtobufProcess):
  
  @ProtobufProcess.install(mesos.internal.ExecutorRegisteredMessage)
  def registered(self, message):
    pass
  
  @ProtobufProcess.install(mesos.internal.ExecutorReregisteredMessage)
  def reregistered(self, message):
    pass
  
  @ProtobufProcess.install(mesos.internal.ReconnectExecutorMessage)
  def reconnect(self, message):
    pass
  
  @ProtobufProcess.install(mesos.internal.RunTaskMessage)
  def run_task(self, message):
    pass
  
  @ProtobufProcess.install(mesos.internal.KillTaskMessage)
  def kill_task(self, message):
    pass
  
  @ProtobufProcess.install(mesos.internal.StatusUpdateAcknowledgementMessage)
  def status_update_acknowledgement(self, message):
    pass
  
  @ProtobufProcess.install(mesos.internal.FrameworkToExecutorMessage)
  def framework_message(self, message):
    pass
  
  @ProtobufProcess.install(mesos.internal.ShutdownExecutorMessage)
  def shutdown(self, message):
    pass
  
  