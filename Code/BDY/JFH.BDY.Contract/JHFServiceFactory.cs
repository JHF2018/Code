using System;
using System.Collections.Generic;
using System.Linq;
using Kingdee.BOS;
using System.Text;
using System.Threading.Tasks;
namespace JFH.BDY.Contracts
{
  public   class JHFServiceFactory
    {


        public static ServicesContainer _mapServer = new ServicesContainer();
        private static bool _noRegistered = true;

        static JHFServiceFactory()
        {
            RegisterService();
        }


        /// <summary>
        /// 注册服务
        /// </summary>
        public static void RegisterService()
        {
            if (_noRegistered)
            {
                _mapServer.Add(typeof(ICommonService), "JFH.BDY.App.Core.CommonService,JFH.BDY.App.Core");

                _noRegistered = false;
            }
        }

        /// <summary>
        /// 获取指定接口实现类实例
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ctx"></param>
        /// <returns></returns>
        public static T GetService<T>(Context ctx)
        {
            if (ctx == null) throw new Exception("{ctx == null}");

            return GetService<T>(ctx, ctx.ServerUrl);
        }

        public static T GetService<T>(Context ctx, string url)
        {
            if (ctx == null) throw new Exception("{ctx == null}");

            if (_noRegistered) RegisterService();

            T service = _mapServer.GetService<T>(typeof(T).AssemblyQualifiedName, url);
            if (service == null) throw new KDException("???", "instance == null");

            return service;
        }

        /// <summary>
        /// 销毁服务类实例
        /// </summary>
        /// <param name="service"></param>
        public static void CloseService(object service)
        {
            IDisposable disposable = service as IDisposable;
            if (disposable != null) disposable.Dispose();
        }

    }
}
