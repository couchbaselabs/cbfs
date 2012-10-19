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
    var format = d3.format(",d");

    var children = [];
    for (var n in d) {
        children.push({age: d[n].hbage_ms,
                       hbs: d[n].hbage_str,
                       uptime: d[n].uptime_str,
                       node: n,
                       avail: d[n].free,
                       value: d[n].size,
                       total: d[n].free + d[n].size});
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

    node.append("path")
        .attr("class", "arc arcf")
        .style("fill", "white")
        .each(function(d) { this._current = {value: 0, r: d.r}; });

    node.append("path")
        .attr("class", "arc arce")
        .style("fill", "#ddd")
        .each(function(d) { this._current = {value: 0, r: d.r}; });

    node.append("text")
        .attr("text-anchor", "middle")
        .attr("dy", ".3em")
        .text(function(d) { return d.node.substring(0, d.r / 3); });

    vis.selectAll("g.node text")
        .data(data, dKey)
        .text(function(d) {
            return d.node + " " + prettySize(d.value) + "/" + prettySize(d.avail);
        });

    function arcTween(a) {
        var i = d3.interpolate(this._current, {r: a.r, value: (a.value / a.total) * (2 * Math.PI)});
        this._current = i(0);
        var isArce = d3.select(this).attr('class').indexOf('arce') >= 0;
        return function(t) {
            var x = i(t);
            var start = 0, end = x.value;
            if (isArce) {
                start = end;
                end = 2 * Math.PI;
            }
            return d3.svg.arc().innerRadius(0).outerRadius(x.r).startAngle(start).endAngle(end)();
        };
    }

    vis.selectAll("g.node .arcf")
        .data(data, dKey)
      .transition()
        .duration(1000)
        .style("fill", function(d) { return fill(d.age); })
        .attrTween("d", arcTween);

    vis.selectAll("g.node .arce")
        .data(data, dKey)
      .transition()
        .duration(1000)
        .style("fill", function(d) { return d3.interpolate(fill(d.age), 'white')(0.8); })
        .attrTween("d", arcTween);

    vis.selectAll("g.node title")
        .data(data, dKey)
        .text(function(d) {
            var rv = "Last heartbeat from " + d.node + " " + d.hbs + " ago";
            if (d.uptime) {
                rv += ", up " + d.uptime;
            }
            return rv;
        });

}

function drawBubbles(d, r) {
    if (!r) {
        r = Math.min(window.innerWidth - 25, window.innerHeight);
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
    return function(d) {
        updateBubbles(bubble, vis, d);
    };
}

function drawSizeChart(d) {
    var h = window.innerHeight - 4, w = 20;

    var svg = d3.select("#size").append("svg")
        .attr("width", w)
        .attr("height", h);

    var classes = ['avail', 'used'];

    var r = svg.selectAll("rect")
        .data(classes)
      .enter().append("rect")
        .attr("class", String)
        .attr("width", w)
        .attr("x", 2)
        .attr("y", 0);
    svg.selectAll("text")
        .data(classes)
        .enter().append("text")
        .attr("class", String)
        .attr("transform", "rotate(90)")
        .attr("x", 10)
        .attr("y", -5);

    function rv(d) {
        var sizes = {totalUsed: 0, totalFree: 0};

        for (var k in d) {
            sizes.totalUsed += d[k].used;
            sizes.totalFree += d[k].free;
        }

        var total = sizes.totalFree + sizes.totalUsed;
        var y = d3.scale.linear().domain([0, total]).rangeRound([h, 2]);

        d3.select("#size rect.used")
            .attr("title", prettySize(sizes.totalUsed))
          .transition().duration(1000)
            .attr("height", y(0))
            .attr("y", y(sizes.totalUsed));
        d3.select("#size rect.avail")
            .attr("title", prettySize(sizes.totalFree))
          .transition().duration(1000)
            .attr("height", y(sizes.totalUsed))
            .attr("y", y(total));

        d3.select("#size text.used")
            .text("Used: " + prettySize(sizes.totalUsed))
          .transition().duration(1000)
            .attr("x", function(d) {
                return Math.min(h - 200, Math.max(y(sizes.totalUsed) + 10, 200));
            });
        d3.select("#size text.avail")
            .text("Available: " + prettySize(sizes.totalFree));
    };
    rv(d);
    return rv;
}

function drawRepcounts(d) {
    var vals = [],
        names = [],
        formatNumber = d3.format(",d");

    if (!d.rows) {
        console.log("No rows found:", d);
        return;
    }

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

    return drawRepcounts;
}

function updateCBFSConfig(d) {
    if (d) {
        cbfsConfig = d;
    }
    return updateCBFSConfig;
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

function updateTasks(d) {
    var tlist = d3.select("#tasklist");
    function rv(json) {

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
            .attr("title", function(d) {
                if (d.value.state == "preparing") {
                    return "preparing";
                }
                return reltime(d.value.ts);
            })
            .attr("class", function(d) { return d.value.state; })
            .text(function(d) { return d.key; });
    };
    rv(d);
    return rv;
}

function initAndRefresh(path, functions, interval) {
    d3.json(path, function(d) {
        if (!d) {
            console.log("Initialization error on", path, "Trying again in one second");
            setTimeout(function() {
                initAndRefresh(path, functions, interval);
            }, 1000);
        }
        var updates = [];
        for (var i = 0; i < functions.length; i++) {
            updates.push(functions[i](d));
        }

        setInterval(function() {
            d3.json(path, function(d) {
                if (d) {
                    for (var i = 0; i < updates.length; i++) {
                        updates[i](d);
                    }
                } else {
                    console.log("Error updating", path);
                }
            });
        }, interval);
    });
}

function monitorInit() {
    console.log("Starting monitoring");

    initAndRefresh("/.cbfs/config/", [updateCBFSConfig], 60000);
    initAndRefresh("/.cbfs/tasks/", [updateTasks], 5000);
    initAndRefresh("/.cbfs/nodes/",
                   [drawBubbles, drawSizeChart], refreshInterval);
    initAndRefresh(repCountURL, [drawRepcounts], refreshInterval);
}
