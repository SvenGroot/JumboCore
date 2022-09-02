// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Indicates that a property should be added to the job settings for a job.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   You can apply this attribute to properties of a job runner class that inherits from <see cref="BaseJobRunner"/>.
    ///   Call the <see cref="BaseJobRunner.ApplyJobPropertiesAndSettings"/> method after creating your <see cref="JobConfiguration"/>
    ///   to add the properties tot the job settings.
    /// </para>
    /// <para>
    ///   The <see cref="Builder.JobBuilderJob"/> class automatically
    ///   do this after the <see cref="JobConfiguration"/> has been created.
    /// </para>
    /// </remarks>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class JobSettingAttribute : Attribute
    {
        private readonly string _key;

        /// <summary>
        /// Initializes a new instance of the <see cref="JobSettingAttribute"/> class that
        /// will use "JobBuilderClassName.PropertyName" as the setting's key.
        /// </summary>
        public JobSettingAttribute()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JobSettingAttribute"/> class with the specified key.
        /// </summary>
        /// <param name="key">The key used for the setting in the settings dictionary.</param>
        public JobSettingAttribute(string key)
        {
            ArgumentNullException.ThrowIfNull(key);
            if (key.Length == 0)
                throw new ArgumentException("The key may not be zero-length.", nameof(key));

            _key = key;
        }

        /// <summary>
        /// Gets the key used for the setting in the settings dictionary.
        /// </summary>
        /// <value>The key for the setting, or <see langword="null" /> if they key should be "JobBuilderClassName.PropertyName".</value>
        public string Key
        {
            get { return _key; }
        }
    }
}
