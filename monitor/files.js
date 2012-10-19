function showFileTreeMap(w, h, json1) {
    function cell() {
        this.style("left", function(d) { return d.x + "px"; })
            .style("top", function(d) { return d.y + "px"; })
            .style("width", function(d) { return Math.max(0, d.dx - 1) + "px"; })
            .style("height", function(d) { return Math.max(0, d.dy - 1) + "px"; })
            .attr("title", function(d) {
                return d.name + ": " + prettySize(d.size) +
                    " / " + d.descendants + " descendants";
            })
            .on('click', function(x) {
                history.pushState({path:x.path}, x.path, window.location + x.path);
                updateFileMap(w, h, x.path);
            });
    }

    function pathJoin(a, b) {
        while (a.charAt(a.length-1) == "/") {
            a = a.slice(0, -1);
        }
        while (b.charAt(0) == "/") {
            b = b.slice(1);
        }
        return a + "/" + b;
    }

    var color = d3.scale.category20c();

    var d3data = {name: json1.path, "children": []};

    for (k in json1.dirs) {
        d3data.children.push({
            name: k,
            children: json1.dirs[k],
            size: json1.dirs[k].size,
            descendants: json1.dirs[k].descendants,
            path: pathJoin(json1.path, k)
        });
    }

    var treemap = d3.layout.treemap()
        .size([w, h])
        .sticky(true)
        .value(function(d) { return d.size; });

    d3.select("#files").data([d3data]).selectAll("div")
        .data(treemap.nodes)
            .attr("class", "cell")
        .style("background", function(d) { return color(d.name);})
        .text(function(d) { return d.name; })
        .call(cell)
      .exit().remove();

    d3.select("#files").data([d3data]).selectAll("div")
        .data(treemap.nodes)
      .enter().append("div")
        .attr("class", "cell")
        .style("background", function(d) { return color(d.name);})
        .text(function(d) { return d.name; })
        .call(cell);
}

function updateFileMap(w, h, path) {
    d3.json("/.cbfs/list" + path, function(j1) {
        showFileTreeMap(w, h, j1);
    });
}

function fileInit(w, h, path) {
    updateFileMap(w, h, path);
}
