// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Enables the use of <see cref="Type.GetType(string)"/> to resolve types in assemblies loaded with <see cref="Assembly.LoadFrom(string)"/>.
    /// </summary>
    public static class AssemblyResolver
    {
        private static bool _registered;

        /// <summary>
        /// Registers the assembly resolver with the current AppDomain.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Register()
        {
            if( !_registered )
            {
                AppDomain.CurrentDomain.AssemblyResolve += new ResolveEventHandler(CurrentDomain_AssemblyResolve);
                _registered = true;
            }
        }

        private static Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            // The TaskHost wants to use Type.GetType to instantiate various types, and it wants to include the
            // assemblies loaded by Assembly.LoadFrom, which isn't done by default. We'll do that here.
            Assembly result = (from assembly in ((AppDomain)sender).GetAssemblies()
                               where assembly.FullName == args.Name || assembly.GetName().Name == args.Name
                               select assembly).SingleOrDefault();
            return result;
        }
    }
}
