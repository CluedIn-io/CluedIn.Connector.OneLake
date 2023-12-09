using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.OneLake.Connector;

namespace CluedIn.Connector.OneLake
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<IOneLakeClient>().ImplementedBy<OneLakeClient>().OnlyNewServices());
            container.Register(Component.For<OneLakeConstants>().ImplementedBy<OneLakeConstants>().LifestyleSingleton());
        }
    }
}
