using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using CluedIn.Connector.OneLake.Connector;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Caching;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.OneLake.Tests.Integration
{

    public class OneLakeConnectorTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public OneLakeConnectorTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async void VerifyStoreData_EventStream()
        {
            var organizationId = Guid.NewGuid();
            var providerDefinitionId = Guid.Parse("36C1B087-97C0-4460-A813-6E4EA1D1BC9A");

            var container = new WindsorContainer();

            container.Register(Component.For<ILogger<OrganizationDataStores>>()
                .Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

            container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
            container.Register(Component.For<IApplicationCache>().Instance(new Mock<IApplicationCache>().Object));

            var applicationContext = new ApplicationContext(container);
            var organization = new Organization(applicationContext, organizationId);

            var logger = new Mock<ILogger>().Object;

            var context = new ExecutionContext(applicationContext, organization, logger);

            var azureDataLakeConstantsMock = new Mock<IOneLakeConstants>();
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);

            var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
            Assert.NotNull(accountName);
            var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
            Assert.NotNull(accountKey);

            var fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
            var directoryName = $"xunit-{DateTime.Now.Ticks}";

            var connectorConnectionMock = new Mock<IConnectorConnectionV2>();
            connectorConnectionMock.Setup(x => x.Authentication).Returns(new Dictionary<string, object>()
            {
                { "AccountName", accountName },
                { "AccountKey", accountKey },
                { "FileSystemName", fileSystemName },
                { "DirectoryName", directoryName },
            });

            var connectorMock = new Mock<OneLakeConnector>(
                new Mock<ILogger<OneLakeConnector>>().Object,
                new OneLakeClient(),
                azureDataLakeConstantsMock.Object
            );
            connectorMock.Setup(x => x.GetAuthenticationDetails(context, providerDefinitionId))
                .Returns(Task.FromResult(connectorConnectionMock.Object));
            connectorMock.CallBase = true;

            var connector = connectorMock.Object;

            var data = new ConnectorEntityData(VersionChangeType.Added, StreamMode.EventStream,
                Guid.Parse("f55c66dc-7881-55c9-889f-344992e71cb8"),
                new ConnectorEntityPersistInfo("etypzcezkiehwq8vw4oqog==", 1), null,
                EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"),
                "/Person",
                new[]
                {
                    new ConnectorPropertyData("user.lastName", "Picard",
                        new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.lastName"))),
                    new ConnectorPropertyData("Name", "Jean Luc Picard",
                        new EntityPropertyConnectorPropertyDataType(typeof(string))),
                },
                new IEntityCode[] { EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0") },
                new[]
                {
                    new EntityEdge(
                        new EntityReference(
                            EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                        new EntityReference(EntityCode.FromKey("/EntityA#Somewhere:1234")), "/EntityA")
                },
                new[]
                {
                    new EntityEdge(new EntityReference(EntityCode.FromKey("/EntityB#Somewhere:5678")),
                        new EntityReference(
                            EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                        "/EntityB")
                });

            var streamModel = new Mock<IReadOnlyStreamModel>();
            streamModel.Setup(x => x.ConnectorProviderDefinitionId).Returns(providerDefinitionId);
            streamModel.Setup(x => x.ContainerName).Returns("test");
            streamModel.Setup(x => x.Mode).Returns(StreamMode.EventStream);

            await connector.StoreData(context, streamModel.Object, data);
            var client = new DataLakeServiceClient(new Uri($"https://{accountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(accountName, accountKey));

            PathItem path;
            DataLakeFileSystemClient fsClient;

            var d = DateTime.Now;
            while (true)
            {
                if (DateTime.Now > d.AddSeconds(30))
                {
                    throw new TimeoutException();
                }

                if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }

                fsClient = client.GetFileSystemClient(fileSystemName);

                var paths = fsClient.GetPaths(recursive: true)
                    .Where(p => p.IsDirectory == false)
                    .Where(p => p.Name.Contains(directoryName))
                    .ToArray();

                if (paths.Length == 0)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                path = paths.Single();

                if (path.ContentLength > 0)
                {
                    break;
                }
            }

            var fileClient = fsClient.GetFileClient(path.Name);

            var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();

            _testOutputHelper.WriteLine(content);

            Assert.Equal($@"[
  {{
    ""user.lastName"": ""Picard"",
    ""Name"": ""Jean Luc Picard"",
    ""Id"": ""f55c66dc-7881-55c9-889f-344992e71cb8"",
    ""PersistHash"": ""etypzcezkiehwq8vw4oqog=="",
    ""OriginEntityCode"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
    ""EntityType"": ""/Person"",
    ""Codes"": [
      ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0""
    ],
    ""ProviderDefinitionId"": ""c444cda8-d9b5-45cc-a82d-fef28e08d55c"",
    ""ContainerName"": ""test"",
    ""OutgoingEdges"": [
      {{
        ""FromReference"": {{
          ""Code"": {{
            ""Origin"": {{
              ""Code"": ""Somewhere"",
              ""Id"": null
            }},
            ""Value"": ""5678"",
            ""Key"": ""/EntityB#Somewhere:5678"",
            ""Type"": {{
              ""IsEntityContainer"": false,
              ""Root"": null,
              ""Code"": ""/EntityB""
            }}
          }},
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/EntityB""
          }},
          ""Name"": null,
          ""Properties"": null,
          ""PropertyCount"": null,
          ""EntityId"": null,
          ""IsEmpty"": false
        }},
        ""ToReference"": {{
          ""Code"": {{
            ""Origin"": {{
              ""Code"": ""Acceptance"",
              ""Id"": null
            }},
            ""Value"": ""7c5591cf-861a-4642-861d-3b02485854a0"",
            ""Key"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
            ""Type"": {{
              ""IsEntityContainer"": false,
              ""Root"": null,
              ""Code"": ""/Person""
            }}
          }},
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/Person""
          }},
          ""Name"": null,
          ""Properties"": null,
          ""PropertyCount"": null,
          ""EntityId"": null,
          ""IsEmpty"": false
        }},
        ""EdgeType"": {{
          ""Root"": null,
          ""Code"": ""/EntityB""
        }},
        ""HasProperties"": false,
        ""Properties"": {{}},
        ""CreationOptions"": 0,
        ""Weight"": null,
        ""Version"": 0
      }}
    ],
    ""IncomingEdges"": [
      {{
        ""FromReference"": {{
          ""Code"": {{
            ""Origin"": {{
              ""Code"": ""Acceptance"",
              ""Id"": null
            }},
            ""Value"": ""7c5591cf-861a-4642-861d-3b02485854a0"",
            ""Key"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
            ""Type"": {{
              ""IsEntityContainer"": false,
              ""Root"": null,
              ""Code"": ""/Person""
            }}
          }},
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/Person""
          }},
          ""Name"": null,
          ""Properties"": null,
          ""PropertyCount"": null,
          ""EntityId"": null,
          ""IsEmpty"": false
        }},
        ""ToReference"": {{
          ""Code"": {{
            ""Origin"": {{
              ""Code"": ""Somewhere"",
              ""Id"": null
            }},
            ""Value"": ""1234"",
            ""Key"": ""/EntityA#Somewhere:1234"",
            ""Type"": {{
              ""IsEntityContainer"": false,
              ""Root"": null,
              ""Code"": ""/EntityA""
            }}
          }},
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/EntityA""
          }},
          ""Name"": null,
          ""Properties"": null,
          ""PropertyCount"": null,
          ""EntityId"": null,
          ""IsEmpty"": false
        }},
        ""EdgeType"": {{
          ""Root"": null,
          ""Code"": ""/EntityA""
        }},
        ""HasProperties"": false,
        ""Properties"": {{}},
        ""CreationOptions"": 0,
        ""Weight"": null,
        ""Version"": 0
      }}
    ],
    ""ChangeType"": ""Added""
  }}
]", content);

            await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
        }

        [Fact]
        public async void VerifyStoreData_Sync()
        {
            var organizationId = Guid.NewGuid();
            var providerDefinitionId = Guid.Parse("36C1B087-97C0-4460-A813-6E4EA1D1BC9A");

            var container = new WindsorContainer();

            container.Register(Component.For<ILogger<OrganizationDataStores>>()
                .Instance(new Mock<ILogger<OrganizationDataStores>>().Object));

            container.Register(Component.For<SystemContext>().Instance(new SystemContext(container)));
            container.Register(Component.For<IApplicationCache>().Instance(new Mock<IApplicationCache>().Object));

            var applicationContext = new ApplicationContext(container);
            var organization = new Organization(applicationContext, organizationId);

            var logger = new Mock<ILogger>().Object;

            var context = new ExecutionContext(applicationContext, organization, logger);

            var azureDataLakeConstantsMock = new Mock<IOneLakeConstants>();
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheRecordsThresholdDefaultValue).Returns(50);
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalKeyName).Returns("abc");
            azureDataLakeConstantsMock.Setup(x => x.CacheSyncIntervalDefaultValue).Returns(2000);

            var accountName = Environment.GetEnvironmentVariable("ADL2_ACCOUNTNAME");
            Assert.NotNull(accountName);
            var accountKey = Environment.GetEnvironmentVariable("ADL2_ACCOUNTKEY");
            Assert.NotNull(accountKey);

            var fileSystemName = $"xunit-fs-{DateTime.Now.Ticks}";
            var directoryName = $"xunit-{DateTime.Now.Ticks}";

            var connectorConnectionMock = new Mock<IConnectorConnectionV2>();
            connectorConnectionMock.Setup(x => x.Authentication).Returns(new Dictionary<string, object>()
            {
                { "AccountName", accountName },
                { "AccountKey", accountKey },
                { "FileSystemName", fileSystemName },
                { "DirectoryName", directoryName },
            });

            var connectorMock = new Mock<OneLakeConnector>(
                new Mock<ILogger<OneLakeConnector>>().Object,
                new OneLakeClient(),
                azureDataLakeConstantsMock.Object
            );
            connectorMock.Setup(x => x.GetAuthenticationDetails(context, providerDefinitionId))
                .Returns(Task.FromResult(connectorConnectionMock.Object));
            connectorMock.CallBase = true;

            var connector = connectorMock.Object;

            var data = new ConnectorEntityData(VersionChangeType.Added, StreamMode.EventStream,
                Guid.Parse("f55c66dc-7881-55c9-889f-344992e71cb8"),
                new ConnectorEntityPersistInfo("etypzcezkiehwq8vw4oqog==", 1), null,
                EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"),
                "/Person",
                new[]
                {
                    new ConnectorPropertyData("user.lastName", "Picard",
                        new VocabularyKeyConnectorPropertyDataType(new VocabularyKey("user.lastName"))),
                    new ConnectorPropertyData("Name", "Jean Luc Picard",
                        new EntityPropertyConnectorPropertyDataType(typeof(string))),
                },
                new IEntityCode[] { EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0") },
                new[]
                {
                    new EntityEdge(
                        new EntityReference(
                            EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                        new EntityReference(EntityCode.FromKey("/EntityA#Somewhere:1234")), "/EntityA")
                },
                new[]
                {
                    new EntityEdge(new EntityReference(EntityCode.FromKey("/EntityB#Somewhere:5678")),
                        new EntityReference(
                            EntityCode.FromKey("/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0")),
                        "/EntityB")
                });

            var streamModel = new Mock<IReadOnlyStreamModel>();
            streamModel.Setup(x => x.ConnectorProviderDefinitionId).Returns(providerDefinitionId);
            streamModel.Setup(x => x.ContainerName).Returns("test");
            streamModel.Setup(x => x.Mode).Returns(StreamMode.Sync);

            await connector.StoreData(context, streamModel.Object, data);
            var client = new DataLakeServiceClient(new Uri($"https://{accountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(accountName, accountKey));

            PathItem path;
            DataLakeFileSystemClient fsClient;

            var d = DateTime.Now;
            while (true)
            {
                if (DateTime.Now > d.AddSeconds(30))
                {
                    throw new TimeoutException();
                }

                if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }

                fsClient = client.GetFileSystemClient(fileSystemName);

                var paths = fsClient.GetPaths(recursive: true)
                    .Where(p => p.IsDirectory == false)
                    .Where(p => p.Name.Contains(directoryName))
                    .ToArray();

                if (paths.Length == 0)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                path = paths.Single();

                if (path.ContentLength > 0)
                {
                    break;
                }
            }

            var fileClient = fsClient.GetFileClient(path.Name);

            var content = new StreamReader(fileClient.Read().Value.Content).ReadToEnd();

            _testOutputHelper.WriteLine(content);

            Assert.Equal($@"{{
  ""user.lastName"": ""Picard"",
  ""Name"": ""Jean Luc Picard"",
  ""Id"": ""f55c66dc-7881-55c9-889f-344992e71cb8"",
  ""PersistHash"": ""etypzcezkiehwq8vw4oqog=="",
  ""OriginEntityCode"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
  ""EntityType"": ""/Person"",
  ""Codes"": [
    ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0""
  ],
  ""ProviderDefinitionId"": ""c444cda8-d9b5-45cc-a82d-fef28e08d55c"",
  ""ContainerName"": ""test"",
  ""OutgoingEdges"": [
    {{
      ""FromReference"": {{
        ""Code"": {{
          ""Origin"": {{
            ""Code"": ""Somewhere"",
            ""Id"": null
          }},
          ""Value"": ""5678"",
          ""Key"": ""/EntityB#Somewhere:5678"",
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/EntityB""
          }}
        }},
        ""Type"": {{
          ""IsEntityContainer"": false,
          ""Root"": null,
          ""Code"": ""/EntityB""
        }},
        ""Name"": null,
        ""Properties"": null,
        ""PropertyCount"": null,
        ""EntityId"": null,
        ""IsEmpty"": false
      }},
      ""ToReference"": {{
        ""Code"": {{
          ""Origin"": {{
            ""Code"": ""Acceptance"",
            ""Id"": null
          }},
          ""Value"": ""7c5591cf-861a-4642-861d-3b02485854a0"",
          ""Key"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/Person""
          }}
        }},
        ""Type"": {{
          ""IsEntityContainer"": false,
          ""Root"": null,
          ""Code"": ""/Person""
        }},
        ""Name"": null,
        ""Properties"": null,
        ""PropertyCount"": null,
        ""EntityId"": null,
        ""IsEmpty"": false
      }},
      ""EdgeType"": {{
        ""Root"": null,
        ""Code"": ""/EntityB""
      }},
      ""HasProperties"": false,
      ""Properties"": {{}},
      ""CreationOptions"": 0,
      ""Weight"": null,
      ""Version"": 0
    }}
  ],
  ""IncomingEdges"": [
    {{
      ""FromReference"": {{
        ""Code"": {{
          ""Origin"": {{
            ""Code"": ""Acceptance"",
            ""Id"": null
          }},
          ""Value"": ""7c5591cf-861a-4642-861d-3b02485854a0"",
          ""Key"": ""/Person#Acceptance:7c5591cf-861a-4642-861d-3b02485854a0"",
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/Person""
          }}
        }},
        ""Type"": {{
          ""IsEntityContainer"": false,
          ""Root"": null,
          ""Code"": ""/Person""
        }},
        ""Name"": null,
        ""Properties"": null,
        ""PropertyCount"": null,
        ""EntityId"": null,
        ""IsEmpty"": false
      }},
      ""ToReference"": {{
        ""Code"": {{
          ""Origin"": {{
            ""Code"": ""Somewhere"",
            ""Id"": null
          }},
          ""Value"": ""1234"",
          ""Key"": ""/EntityA#Somewhere:1234"",
          ""Type"": {{
            ""IsEntityContainer"": false,
            ""Root"": null,
            ""Code"": ""/EntityA""
          }}
        }},
        ""Type"": {{
          ""IsEntityContainer"": false,
          ""Root"": null,
          ""Code"": ""/EntityA""
        }},
        ""Name"": null,
        ""Properties"": null,
        ""PropertyCount"": null,
        ""EntityId"": null,
        ""IsEmpty"": false
      }},
      ""EdgeType"": {{
        ""Root"": null,
        ""Code"": ""/EntityA""
      }},
      ""HasProperties"": false,
      ""Properties"": {{}},
      ""CreationOptions"": 0,
      ""Weight"": null,
      ""Version"": 0
    }}
  ]
}}", content);

            data.ChangeType = VersionChangeType.Removed;
            await connector.StoreData(context, streamModel.Object, data);

            d = DateTime.Now;
            while (true)
            {
                if (DateTime.Now > d.AddSeconds(30))
                {
                    throw new TimeoutException("Timeout waiting for file to be deleted");
                }

                if (client.GetFileSystems().All(fs => fs.Name != fileSystemName))
                {
                    continue;
                }

                fsClient = client.GetFileSystemClient(fileSystemName);

                var paths = fsClient.GetPaths(recursive: true)
                    .Where(p => p.IsDirectory == false)
                    .Where(p => p.Name.Contains(directoryName))
                    .Select(p => p.Name)
                    .ToArray();

                if (paths.Contains(path.Name))
                {
                    Thread.Sleep(1000);
                    continue;
                }

                break;
            }

            await fsClient.GetDirectoryClient(directoryName).DeleteAsync();
        }
    }
}
