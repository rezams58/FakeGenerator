namespace FakeOrderGenerator.Models;

public sealed class Partitioning
{
    public Guid EventId { get; set; }
    public string EventType { get; set; } = "OrderCreated";
    public int EventVersion { get; set; } = 1;
    public DateTime CreatedAt { get; set; }
    public OrderData Data { get; set; } = new();
}
public sealed class OrderData
{
    public string OrderNumber { get; set; } = "";
    public int CustomerId { get; set; }
    public string NationalCode { get; set; } = "";
    public DateTime OrderDate { get; set; }
    public decimal TotalAmount { get; set; }
    public string OrderStatus { get; set; } = "";
    public string PaymentMethod { get; set; } = "";
    public string City { get; set; } = "";
    public bool IsPaid { get; set; }
}
