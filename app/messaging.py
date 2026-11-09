from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue


DLX = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
DEAD_QUEUE = RabbitQueue("payments.dead", durable=True, routing_key="payments.dead")
NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": DLX.name,
        "x-dead-letter-routing-key": "payments.dead",
    },
)


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(DLX)
    dead_queue = await broker.declare_queue(DEAD_QUEUE)
    await dead_queue.bind(exchange, routing_key="payments.dead")
    await broker.declare_queue(NEW_QUEUE)
