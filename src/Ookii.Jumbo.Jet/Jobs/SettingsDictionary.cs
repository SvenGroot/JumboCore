// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Xml;
using System.Xml.Serialization;

namespace Ookii.Jumbo.Jet.Jobs;

/// <summary>
/// Provides settings for a job configuration.
/// </summary>
public sealed class SettingsDictionary : Dictionary<string, string>, IXmlSerializable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SettingsDictionary"/> class.
    /// </summary>
    public SettingsDictionary()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SettingsDictionary"/> class with elements that are copied from the specified <see cref="IDictionary{TKey,TValue}"/>.
    /// </summary>
    /// <param name="dictionary">The <see cref="IDictionary{TKey,TValue}"/> whose elements are copied to the <see cref="SettingsDictionary"/>.</param>
    public SettingsDictionary(IDictionary<string, string> dictionary)
        : base(dictionary)
    {
    }

    #region IXmlSerializable Members

    System.Xml.Schema.XmlSchema? IXmlSerializable.GetSchema()
    {
        return null;
    }

    void IXmlSerializable.ReadXml(XmlReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        var startElementName = reader.Name;
        var depth = reader.Depth;
        if (reader.IsEmptyElement)
        {
            reader.ReadStartElement();
            return;
        }

        reader.ReadStartElement();
        while (!(reader.NodeType == XmlNodeType.EndElement && reader.Name == startElementName && reader.Depth == depth))
        {
            if (reader.IsStartElement("Setting", JobConfiguration.XmlNamespace))
                Add(reader.GetAttribute("key")!, reader.GetAttribute("value")!);
            reader.Read();
        }
        reader.ReadEndElement();
    }

    void IXmlSerializable.WriteXml(XmlWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        foreach (var item in this)
        {
            writer.WriteStartElement("Setting", JobConfiguration.XmlNamespace);
            writer.WriteAttributeString("key", item.Key);
            writer.WriteAttributeString("value", item.Value);
            writer.WriteEndElement();
        }
    }

    #endregion

    /// <summary>
    /// Adds a setting with the specified type.
    /// </summary>
    /// <param name="key">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    public void AddSetting(string key, object value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        Add(key, (string?)TypeDescriptor.GetConverter(value).ConvertTo(null, System.Globalization.CultureInfo.InvariantCulture, value, typeof(string)) ?? string.Empty);
    }

    /// <summary>
    /// Gets a setting with the specified type and default value.
    /// </summary>
    /// <typeparam name="T">The type of the setting.</typeparam>
    /// <param name="key">The name of the setting.</param>
    /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
    /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
    public T? GetSetting<T>(string key, T? defaultValue)
    {
        if (TryGetValue(key, out var value))
        {
            return (T?)TypeDescriptor.GetConverter(typeof(T)).ConvertFrom(null, System.Globalization.CultureInfo.InvariantCulture, value);
        }
        else
            return defaultValue;
    }

    /// <summary>
    /// Tries to get a setting with the specified type.
    /// </summary>
    /// <typeparam name="T">The type of the setting.</typeparam>
    /// <param name="key">The name of the setting..</param>
    /// <param name="value">If the function returns <see langword="true"/>, receives the value of the setting.</param>
    /// <returns><see langword="true"/> if the settings dictionary contained the specified setting; otherwise, <see langword="false"/>.</returns>
    public bool TryGetSetting<T>(string key, out T? value)
    {
        if (TryGetValue(key, out var stringValue))
        {
            value = (T?)TypeDescriptor.GetConverter(typeof(T)).ConvertFrom(null, System.Globalization.CultureInfo.InvariantCulture, stringValue);
            return true;
        }
        else
        {
            value = default(T);
            return false;
        }

    }

    /// <summary>
    /// Gets a string setting with the specified default value.
    /// </summary>
    /// <param name="key">The name of the setting.</param>
    /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
    /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
    [return: NotNullIfNotNull(nameof(defaultValue))]
    public string? GetSetting(string key, string? defaultValue)
    {
        if (TryGetValue(key, out var value))
            return value;
        else
            return defaultValue;
    }

    /// <summary>
    /// Gets a setting's string value with the specified default value, checking first in the stage settings and then in the job settings.
    /// </summary>
    /// <param name="job">The job configuration.</param>
    /// <param name="stage">The stage stage configuration.</param>
    /// <param name="key">The name of the setting.</param>
    /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
    /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in either the stage or job settings.</returns>
    [return: NotNullIfNotNull(nameof(defaultValue))]
    public static string? GetJobOrStageSetting(JobConfiguration job, StageConfiguration stage, string key, string? defaultValue)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(stage);

        var value = stage.GetSetting(key, null);
        if (value == null)
            value = job.GetSetting(key, defaultValue);

        return value;
    }

    /// <summary>
    /// Gets a setting with the specified type and default value, checking first in the stage settings and then in the job settings.
    /// </summary>
    /// <typeparam name="T">The type of the setting.</typeparam>
    /// <param name="job">The job configuration.</param>
    /// <param name="stage">The stage stage configuration.</param>
    /// <param name="key">The name of the setting.</param>
    /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary" />.</param>
    /// <returns>
    /// The value of the setting, or <paramref name="defaultValue" /> if the setting was not present in either the stage or job settings.
    /// </returns>
    public static T? GetJobOrStageSetting<T>(JobConfiguration job, StageConfiguration stage, string key, T? defaultValue)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(stage);

        if (!stage.TryGetSetting(key, out T? value) && !job.TryGetSetting(key, out value))
            return defaultValue;
        else
            return value;
    }
}
