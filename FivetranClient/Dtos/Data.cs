namespace FivetranClient.Dtos;

public class Data<T>
{
    public List<T> Items { get; set; }
    public string NextCursor { get; set; }
}