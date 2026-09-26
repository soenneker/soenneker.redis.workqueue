using System.Text.Json;
using System.Text.Json.Serialization;

namespace 
Soenneker.Redis.WorkQueue.Tests
;

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(TestWork))]
[JsonSerializable(typeof(string))]
internal partial class TestJsonContext : JsonSerializerContext;
