
$(document).ready(function ()
{
    $("#FileSystemRoot > img").click(toggleDirectory);
    getDirectoryListing("/", $("#FileSystemRoot"));
    $("input").focus(function() { $(this).select(); });
});

function getDirectoryListing(path, li)
{
    jsonAjax("Api/FileSystem", { "path": path }, function (result)
        {
            var ul = $("<ul>");
            $(li).children("span").children("img").attr("src", "images/arrow_open.png");
            if( result.children && result.children.length > 0 )
            {
                $.each(result.children, function(index, child)
                {
                    var img = $("<img>").attr("alt", child.fullPath).attr("title", child.fullPath);
                    var outerSpan = $("<span>");
                    var span = $("<span>").addClass("fileSystemEntry").append(img).appendTo(outerSpan);
                    span.append(document.createTextNode(child.name + " (" + child.dateCreated));
                    if( child.isDirectory )
                    {
                        img.attr("src", "images/folder_open.png");
                        $("<img>").attr("src", "images/arrow_closed.png").attr("alt", "expand").click(toggleDirectory).addClass("directory").prependTo(outerSpan);
                    }
                    else
                    {
                        img.attr("src", "images/generic_document.png");
                        span.append(document.createTextNode("; "));
                        span.append($("<abbr>").attr("title", child.sizeInBytes + " bytes").text(child.formattedSize));
                        span.css("margin-left", "21px");
                    }
                    span.append(document.createTextNode(")"));
                    span.click(function()
                    {
                        $("#EntryName").text(child.name);
                        $("#EntryCreated").text(child.dateCreated);
                        $("#FullPath").attr("value", child.fullPath);
                        if( child.isDirectory )
                            $("#FileInfo").hide();
                        else
                        {
                            $("#FileInfo").show();
                            $("#FileFormattedSize").text(child.formattedSize);
                            $("#FileSize").text(child.sizeInBytes);
                            $("#BlockSize").text(child.blockSize);
                            $("#BlockCount").text(child.blockCount);
                            $("#ReplicationFactor").text(child.replicationFactor);
                            $("#RecordOptions").text(child.recordOptions);
                            $("#BlockSize").text(child.blockSize);
                            $("#ViewFileLink").attr("href", "ViewFile?path=" + encodeURIComponent(child.fullPath) + "&maxSize=100KB&tail=false");
                            $("#DownloadLink").attr("href", "Api/Download?path=" + encodeURIComponent(child.fullPath));
                        }
                    });

                    ul.append($("<li>").append(outerSpan));
                });
            }

            ul.hide();
            $(li).append(ul);
            ul.show("normal");
        }, "Could not load directory contents.");
}

function toggleDirectory()
{
    var li = $(this).parent().parent();
    var listing = li.children("ul");
    if( listing.length > 0 )
    {
        $(this).attr("src", "images/arrow_closed.png");
        listing.hide("normal", function() { $(listing).remove(); });
    }
    else
    {
        getDirectoryListing($(this).next("span").children("img").attr("alt"), li);
    }
}

function jsonAjax(url, params, success, errorMessage)
{
    return $.ajax({ 
        type: "GET",
        url: url,
        data: params,
        dataType: "json",
        async: true,
        cache: false,
        success: success,
        beforeSend: function(x, s) { x.params = params; },
        error: function(x, e) 
        { 
            if( e !== "abort" )
            {
                if( x.responseText )
                    alert(errorMessage + ": " + $.parseJSON(x.responseText).Message);
                else
                    alert(errorMessage + ".");
            }
        }
    });
}