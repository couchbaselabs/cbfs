var repCountURL = '/.cbfs/viewproxy/cbfs/_design/cbfs/_view/repcounts?group_level=1';
var refreshInterval = 2000;

var cbfsConfig = {};

function prettySize(s) {
    if (s < 10) {
        return s + "B";
	}
	var e = parseInt(Math.floor(Math.log(s) / Math.log(1024)));
    var sizes = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
	var suffix = sizes[parseInt(e)];
	var val = s / Math.pow(1024, Math.floor(e));
    return val.toFixed(2) + suffix;
}

function updateBubbles(bubble, vis, d) {
    if (!d) {
        return;
    }
    var format = d3.format(",d");

    var children = [];
    for (var n in d) {
        children.push({age: d[n].hbage_ms,
                       hbs: d[n].hbage_str,
                       node: n,
                       avail: d[n].free,
                       value: d[n].size});
    }

    function fill(val) {
        var offset = Math.min(1, val / 180000);
        return d3.interpolateRgb("#bbf", "#f55")(offset);
    }

    var data = bubble.nodes({children: children})
        .filter(function(d) { return !d.children; });

    var dKey = function(d) { return d.node; };

    var node = vis.selectAll("g.node")
        .data(data, dKey)
      .enter().append("g")
        .attr("class", "node")
        .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });

    vis.selectAll("g.node")
        .data(data, dKey)
      .transition()
        .duration(1000)
        .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });

    vis.selectAll("g.node")
        .data(data, dKey)
      .exit().transition()
        .duration(1000)
        .attr("transform", function(d) {
            return "translate(" + d.x + "," + vis.attr("height") + ")";
        })
        .ease('quad')
        .remove();

    node.append("title")
        .text(function(d) { return d.node + ": " + format(d.value); });

    node.append("circle")
        .attr("r", function(d) { return d.r; })
        .style("fill", function(d) { return fill(d.age); });

    node.append("text")
        .attr("text-anchor", "middle")
        .attr("dy", ".3em")
        .text(function(d) { return d.node.substring(0, d.r / 3); });

    vis.selectAll("g.node text")
        .data(data, dKey)
        .text(function(d) {
            return d.node + " " + prettySize(d.value) + "/" + prettySize(d.avail);
        });

    vis.selectAll("g.node circle")
        .data(data, dKey)
      .transition()
        .duration(1000)
        .style("fill", function(d) {
            return fill(d.age);
        })
        .attr("r", function(d) { return d.r; });

    vis.selectAll("g.node title")
        .data(data, dKey)
        .text(function(d) {
            return "Last heartbeat from " + d.node + " " + d.hbs + " ago";
        });

}

function drawBubbles(d, r) {
    if (!r) {
        r = Math.min(window.innerWidth, window.innerHeight);
    }
    var bubble = d3.layout.pack()
        .sort(null)
        .size([r, r])
        .padding(1.5);

    var vis = d3.select("#bubbles").append("svg")
        .attr("width", r)
        .attr("height", r)
        .attr("class", "bubble");

    updateBubbles(bubble, vis, d);
    setInterval(function() {
        d3.json("/.cbfs/nodes/", function(d) {
            updateBubbles(bubble, vis, d);
        });
    }, refreshInterval);
}

function drawRepcounts(d) {
    var vals = [],
        names = [],
        formatNumber = d3.format(",d");

    for (var i = 0; i < d.rows.length; i++) {
        names.push(d.rows[i].key);
        vals.push(parseInt(d.rows[i].value));
    }

    var w = 200, bh = 20;

    var x = d3.scale.linear()
        .domain([0, d3.max(vals)])
        .range([0, w]);

    var textify = function(d, i) {
        return names[i] + " rep: " + formatNumber(d);
    };

    var repChart = d3.select("#repcounts svg");

    var dKey = function(d, i) { return names[i]; };

    repChart.selectAll("rect")
        .data(vals, dKey)
      .enter().append("rect")
        .attr("y", function(d, i) { return i * bh; })
        .attr("width", x)
        .attr("x", 0)
        .attr("height", bh);

    repChart.selectAll("rect")
        .data(vals, dKey)
      .exit().remove();

    repChart.selectAll("rect")
        .data(vals, dKey)
        .attr("class", function(d, i) {
            return parseInt(names[i]) < (cbfsConfig.minrepl || 2) ? "under" : null;
        })
      .transition()
        .attr("width", x)
        .attr("x", 0);

    repChart.selectAll("text")
        .data(vals, dKey)
      .enter().append("text");

    repChart.selectAll("text")
        .data(vals, dKey)
      .exit().remove();

    repChart.selectAll("text")
        .data(vals, dKey)
        .attr("x", 10)
        .attr("y", function(d, i) { return bh * (1 + i);})
        .attr("dx", -3)
        .attr("dy", "-5")
        .attr("text-anchor", "start")
        .attr("class", function(d, i) {
            return (parseInt(names[i]) < (cbfsConfig.minrepl || 2) && x(d,i) < 10) ? "under" : null;
        })
        .text(textify);
}

function updateCBFSConfig() {
    d3.json("/.cbfs/config/", function(d) {
        if (d) {
            cbfsConfig = d;
        }
    });
}

function reltime(time){
    var date = new Date(time.replace(/-/g,"/").replace("T", " ").replace("Z", " +0000").replace(/(\d*\:\d*:\d*)\.\d*/g,"$1")),
        diff = (((new Date()).getTime() - date.getTime()) / 1000),
        day_diff = Math.floor(diff / 86400);

    if (isNaN(day_diff)) return time;

    return day_diff < 1 && (
        diff < 60 && "just now" ||
            diff < 120 && "1 minute ago" ||
            diff < 3600 && Math.floor( diff / 60 ) + " minutes ago" ||
            diff < 7200 && "1 hour ago" ||
            diff < 86400 && Math.floor( diff / 3600 ) + " hours ago") ||
        day_diff == 1 && "yesterday" ||
        day_diff < 21 && day_diff + " days ago" ||
        day_diff < 45 && Math.ceil( day_diff / 7 ) + " weeks ago" ||
        time;
}

function updateTasks() {
    var tlist = d3.select("#tasklist");
    d3.json("/.cbfs/tasks/", function(json) {

        d3.select("#taskhdr")
            .style("display", d3.entries(json).length > 0 ? 'block' : 'none');

        tlist.selectAll("li.toplevel")
            .data(d3.keys(json), String)
            .exit().remove();

        var iul = tlist.selectAll("li")
            .data(d3.keys(json), String)
          .enter().append("li")
            .attr("class", "toplevel")
            .text(String)
            .append("ul");

        iul.selectAll("li.toplevel")
            .data(function(d) { return d3.entries(json[d]); })
          .enter().append("li");

        tlist.selectAll("li.toplevel")
            .data(d3.keys(json), String)
          .selectAll("ul li")
            .data(function(d) { return d3.entries(json[d]); })
            .attr("title", function(d) { return reltime(d.value); })
            .text(function(d) { return d.key; });
    });
}

function monitorInit() {
    console.log("Starting monitoring");

    var repChart = d3.select("#repcounts").append("svg")
        .attr("class", "chart")
        .attr("width", 200);

    updateCBFSConfig();
    setInterval(function() {
        updateCBFSConfig();
    }, 60000);

    setInterval(function() {
        updateTasks();
    }, 5000);

    d3.json("/.cbfs/nodes/", drawBubbles);
    setInterval(function() {
        d3.json(repCountURL, drawRepcounts);
    }, refreshInterval);
}

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