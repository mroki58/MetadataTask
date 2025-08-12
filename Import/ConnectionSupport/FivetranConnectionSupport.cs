using FivetranClient;
using FivetranClient.Models;
using Import.Helpers.Fivetran;
using System.Text;

namespace Import.ConnectionSupport;

// equivalent of database is group in Fivetran terminology
public class FivetranConnectionSupport : IConnectionSupport
{
    public const string ConnectorTypeCode = "FIVETRAN";
    private RestApiManager? restApiManager;
    private record FivetranConnectionDetailsForSelection(string ApiKey, string ApiSecret);

    public object? GetConnectionDetailsForSelection()
    {
        Console.Write("Provide your Fivetran API Key: ");
        var apiKey = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(apiKey))
            throw new ArgumentException("API Key cannot be empty.", nameof(apiKey));

        Console.Write("Provide your Fivetran API Secret: ");
        var apiSecret = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(apiSecret))
            throw new ArgumentException("API Secret cannot be empty.", nameof(apiSecret));

        return new FivetranConnectionDetailsForSelection(apiKey, apiSecret);
    }

    public object GetConnection(object? connectionDetails, string? selectedToImport)
    {
        if (connectionDetails is not FivetranConnectionDetailsForSelection details)
        {
            throw new ArgumentException("Invalid connection details provided.");
        }


        return new RestApiManagerWrapper(
            GetOrCreateApiManager(details),
            selectedToImport ?? throw new ArgumentNullException(nameof(selectedToImport)));
    }

    public void CloseConnection(object? connection)
    {
        if (connection is not IDisposable disposable)
        {
            throw new ArgumentException("Invalid connection type provided.");
        }
        disposable.Dispose();
        restApiManager = null;
    }


    public string SelectToImport(object? connectionDetails)
    {
        var details = ValidateConnectionDetails(connectionDetails);
        using var restApiManager = GetOrCreateApiManager(details);

        var groups = GetGroups(restApiManager);
        DisplayGroups(groups);

        var selectedIndex = PromptUserForSelection(groups.Count());
        return groups.ElementAt(selectedIndex - 1).Id;
    }

    public void RunImport(object? connection)
    {
        var wrapper = ValidateConnection(connection);

        var connectors = GetConnectors(wrapper.RestApiManager, wrapper.GroupId);
        DisplayLineageMappings(wrapper.RestApiManager, connectors);
    }

    // --- Helper methods ---
    private FivetranConnectionDetailsForSelection ValidateConnectionDetails(object? connectionDetails)
    {
        if (connectionDetails is not FivetranConnectionDetailsForSelection details)
        {
            throw new ArgumentException("Invalid connection details provided.");
        }
        return details;
    }

    private RestApiManagerWrapper ValidateConnection(object? connection)
    {
        if (connection is not RestApiManagerWrapper wrapper)
        {
            throw new ArgumentException("Invalid connection type provided.");
        }
        return wrapper;
    }

    private RestApiManager GetOrCreateApiManager(FivetranConnectionDetailsForSelection details)
    {
        if (restApiManager == null)
            restApiManager = new RestApiManager(details.ApiKey, details.ApiSecret, TimeSpan.FromSeconds(40));
        return restApiManager;
    }

    private IEnumerable<Group> GetGroups(RestApiManager manager)
    {
        var groups = manager.GetGroupsAsync(CancellationToken.None).ToBlockingEnumerable();
        if (!groups.Any())
        {
            throw new Exception("No groups found in Fivetran account.");
        }
        return groups;
    }

    private void DisplayGroups(IEnumerable<Group> groups)
    {
        var buffer = new StringBuilder();
        buffer.AppendLine("Available groups in Fivetran account:");
        var index = 1;
        foreach (var group in groups)
        {
            buffer.AppendLine($"{index++}. {group.Name} (ID: {group.Id})");
        }
        buffer.Append("Please select a group to import from (by number): ");
        Console.Write(buffer.ToString());
    }

    private int PromptUserForSelection(int maxIndex)
    {
        var input = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(input)
            || !int.TryParse(input, out var selectedIndex)
            || selectedIndex < 1
            || selectedIndex > maxIndex)
        {
            throw new ArgumentException("Invalid group selection.");
        }
        return selectedIndex;
    }

    private IEnumerable<Connector> GetConnectors(RestApiManager manager, string groupId)
    {
        var connectors = manager.GetConnectorsAsync(groupId, CancellationToken.None).ToBlockingEnumerable();
        if (!connectors.Any())
        {
            throw new Exception("No connectors found in the selected group.");
        }
        return connectors;
    }


    private async void DisplayLineageMappings(RestApiManager manager, IEnumerable<Connector> connectors)
    {
        var buffer = new StringBuilder();
        buffer.AppendLine("Lineage mappings:");

        var tasks = connectors.Select(async connector =>
        {
            var connectorSchemas = await manager.GetConnectorSchemasAsync(connector.Id, CancellationToken.None);

            foreach (var schema in connectorSchemas?.Schemas ?? [])
            {
                foreach (var table in schema.Value?.Tables ?? [])
                {
                    lock (buffer)
                    {
                        buffer.AppendLine(
                            $"  {connector.Id}: {schema.Key}.{table.Key} -> {schema.Value?.NameInDestination}.{table.Value.NameInDestination}");
                    }
                }
            }
        });

        await Task.WhenAll(tasks);
        Console.WriteLine(buffer.ToString());
    }


}