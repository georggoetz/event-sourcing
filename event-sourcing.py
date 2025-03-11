class Event:
  def __init__(self, event_type, data, version):
    self.event_type = event_type
    self.data = data
    self.version = version


class EventStore:
  def __init__(self):
    self.events = []
    self.snapshots = {}

  def add_event(self, event):
    self.events.append(event)

  def get_events(self, aggregate_id, after_version=0):
    return [event for event in self.events if event.data['id'] == aggregate_id and event.version > after_version]

  def save_snapshot(self, aggregate_id, snapshot):
    self.snapshots[aggregate_id] = snapshot

  def get_snapshot(self, aggregate_id):
    return self.snapshots.get(aggregate_id, None)


class AccountEntity:
  def __init__(self, acoount_id, balance, version):
    self.account_id = acoount_id
    self.balance = balance
    self.version = version

  def __str__(self):
    return (f'Account ID: {self.account_id}, Balance: {self.balance}, Version: {self.version}')


class EntityStore:
  def __init__(self):
    self.accounts = {}

  def update_entity(self, account_id, balance, version):
    self.accounts[account_id] = AccountEntity(account_id, balance, version)

  def query_entity(self, account_id):
    return self.accounts.get(account_id)


class AccountAggregate:
  def __init__(self, account_id):
    self.account_id = account_id
    self.balance = 0
    self.version = 0

  def apply_event(self, event):
    if event.event_type == 'Deposit':
      self.balance += event.data['amount']
    elif event.event_type == 'Withdrawal':
      self.balance -= event.data['amount']
    self.version = event.version

  def can_withdraw(self, amount):
    return self.balance >= amount

  def __str__(self):
    return (f'Account ID: {self.account_id}, Balance: {self.balance}, Version: {self.version}')


class Command:
  def execute(self):
    raise NotImplementedError()

  def undo(self):
    raise NotImplementedError()


class InsufficientBalanceError(Exception):
  pass


class CommandModel:
  def __init__(self, event_store, query_model, entity_store):
      self.event_store = event_store
      self.entity_store = entity_store
      self.query_model = query_model
      self.version_counter = 0

  def execute_command(self, command):
    if isinstance(command, WithdrawalCommand):
      account = self.query_model.rebuild_aggregate(command.account_id)
      if not account.can_withdraw(command.amount):
        raise InsufficientBalanceError(f'Insufficient balance for withdrawal of {command.amount}')
    self.version_counter += 1
    command.execute(self.version_counter)
    account = self.query_model.rebuild_aggregate(command.account_id)
    self.entity_store.update_entity(command.account_id, account.balance, self.version_counter)

  def undo_command(self, command):
    self.version_counter += 1
    command.undo(self.version_counter)
    self.entity_store.update_entity(command.account_id, account.balance, self.version_counter)


class DepositCommand(Command):
  def __init__(self, event_store, account_id, amount):
    self.event_store = event_store
    self.account_id = account_id
    self.amount = amount

  def execute(self, version):
    event = Event('Deposit', {'id': self.account_id, 'amount': self.amount}, version)
    self.event_store.add_event(event)

  def undo(self, version):
    event = Event('Withdrawal', {'id': self.account_id, 'amount': self.amount}, version)
    self.event_store.add_event(event)


class WithdrawalCommand(Command):
  def __init__(self, event_store, account_id, amount):
    self.event_store = event_store
    self.account_id = account_id
    self.amount = amount

  def execute(self, version):
    event = Event('Withdrawal', {'id': self.account_id, 'amount': self.amount}, version)
    self.event_store.add_event(event)

  def undo(self, version):
    event = Event('Deposit', {'id': self.account_id, 'amount': self.amount}, version)
    self.event_store.add_event(event)


class QueryModel:
  def __init__(self, event_store):
    self.event_store = event_store

  def rebuild_aggregate(self, account_id):
    snapshot = self.event_store.get_snapshot(account_id)
    version = snapshot['version'] if snapshot else 0
    account = AccountAggregate(account_id)
    if snapshot:
      account.balance = snapshot['balance']
      account.version = snapshot['version']
    events = self.event_store.get_events(account_id, after_version=version)
    for event in events:
      account.apply_event(event)
    return account

  def save_snapshot(self, account):
    snapshot = {'balance': account.balance, 'version': account.version}
    self.event_store.save_snapshot(account.account_id, snapshot)


class TransferSaga:
  def __init__(self, command_model):
    self.command_model = command_model
    self.successful_commands = []

  def start_transfer(self, from_account_id, to_account_id, amount):
    withdrawal_command = WithdrawalCommand(self.command_model.event_store, from_account_id, amount)
    deposit_command = DepositCommand(self.command_model.event_store, to_account_id, amount)
    try:
      self.command_model.execute_command(withdrawal_command)
      self.successful_commands.append(withdrawal_command)
      self.command_model.execute_command(deposit_command)
      self.successful_commands.append(deposit_command)
    except Exception as e:
      self.compensate_transfer()
      raise e

  def compensate_transfer(self):
    for command in reversed(self.successful_commands):
      self.command_model.undo_command(command)
    self.successful_commands.clear()


# Example Usage
if __name__ == '__main__':
  event_store = EventStore()
  entity_store = EntityStore()
  query_model = QueryModel(event_store)
  command_model = CommandModel(event_store, query_model, entity_store)

  # Deposit 150
  deposit_command = DepositCommand(event_store, 1, 150)
  command_model.execute_command(deposit_command)

  # Withdraw 50
  withdrawal_command = WithdrawalCommand(event_store, 1, 50)
  command_model.execute_command(withdrawal_command)

  # Attempt to withdraw more than the balance, will be rejected by business logic
  withdrawal_command = WithdrawalCommand(event_store, 1, 150)
  try:
    command_model.execute_command(withdrawal_command)
  except InsufficientBalanceError as e:
    print(e)

  # Query by replaying events: balance should be 100
  account = query_model.rebuild_aggregate(1)
  print(account)

  # Save snapshot
  query_model.save_snapshot(account)

  # Balance should still be 100
  account = query_model.rebuild_aggregate(1)
  print(account)

  # Undo last command (Withdrawal)
  deposit_command = DepositCommand(event_store, 1, 150)
  command_model.execute_command(deposit_command)
  command_model.undo_command(deposit_command)

  # Balance should still be 100
  account = query_model.rebuild_aggregate(1)
  print(account)

  # Query account by fetching entity
  account = entity_store.query_entity(1)
  print(account)

  # Create second account and deposit of 50
  deposit_command = DepositCommand(event_store, 2, 50)
  command_model.execute_command(deposit_command)

  account = query_model.rebuild_aggregate(2)
  print(account)

  # Transfer Saga: transfer 50 from account 1 to account 2
  transfer_saga = TransferSaga(command_model)
  transfer_saga.start_transfer(1, 2, 50)

  account = query_model.rebuild_aggregate(1)
  print(account)

  account = query_model.rebuild_aggregate(2)
  print(account)

  # Transfer Saga: transfer 100 from account 1 to account 2, will fail due to insufficient balance
  transfer_saga = TransferSaga(command_model)
  try:
    transfer_saga.start_transfer(1, 2, 100)
  except InsufficientBalanceError as e:
    print(e)

  account = query_model.rebuild_aggregate(1)
  print(account)

  account = query_model.rebuild_aggregate(2)
  print(account)
