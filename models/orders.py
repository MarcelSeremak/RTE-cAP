from datetime import datetime
from pydantic import BaseModel, Field

class OrderEvent(BaseModel):
    order_id: int
    customer_id: int
    product_id: int
    quantity: int
    unit_price: float
    currency: str
    timestamp: datetime