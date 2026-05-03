using Microsoft.AspNetCore.Mvc.Testing;

namespace BureauObservability.Tests;

public class ProgramTests(WebApplicationFactory<Program> factory) : IClassFixture<WebApplicationFactory<Program>>
{
    [Fact]
    public async Task Get_Root_ReturnsOk()
    {
        var client = factory.CreateClient();
        var response = await client.GetAsync("/");
        Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
    }
}
