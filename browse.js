var allMediaURL = '/.cbfs/viewproxy/cbfs/_design/media/_view/media?group_level=3';

function toTitleCase(str) {
    return str.replace(/\w\S*/g, function(txt) {
        return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
    });
}

function lower(str) {
    return str.toLowerCase();
}

function identity(s) {
    return s;
}

function doList(target, vals, conv) {
    for (var i = 0; i < vals.length;) {
        var r = vals[i];
        var h = "<li>" + conv(r.key[0]) + "<ul>";
        var model = r.key[0];

        while (i < vals.length && r.key[0] === model) {
            h += "<li>" + conv(r.key[1]) + " (" + r.value.count  + ")</li>";
            i++;
            r = vals[i];
        }

        h += "</ul></li>";
        $(target).append(h);
    }
}

function initStuff() {
    $.ajax(allMusicURL + '?group_level=2', {
        success: function(d) {
            doList('#music', JSON.parse(d).rows, identity);
        }
    });

    $.ajax(allPicsURL + '?group_level=2', {
        success: function(d) {
            doList('#pictures', JSON.parse(d).rows, lower);
        }
    });
}

function computeNodesLinks(rows) {
    var rv = {nodes: [], links: []};
    var sizes = {};

    for (var i = 0; i < rows.length; i++) {
        var k = rows[i].key[1];
        if (rows[i].key[0] === 'Picture') {
            k = toTitleCase(k);
        }
        sizes[k] = (sizes[k] || 0) + rows[i].value.count;
    }

    var seen = {};

    var prevmodel = "";
    var prevsection = "";
    var j = 0;
    var base = 0;

    for (var i = 0; i < rows.length; i++) {
        var r = rows[i];
        var section = r.key[0];
        var model = section == 'Picture' ? toTitleCase(r.key[1]) : r.key[1];

        if (section != prevsection) {
            prevsection = section;
            rv.nodes.push({name: section, size: 10, type: "root"});
            base = j;
            j++;
        }

        if (model != prevmodel) {
            prevmodel = model;

            rv.nodes.push({name: model, size: sizes[model], type: "make"});
            rv.links.push({source: j, target: base, value: 1});
            seen[model] = j;
            j++;
        }

        var name = (section == 'Picture') ? toTitleCase(r.key[2]) : r.key[2];

        rv.nodes.push({name: name, size: r.value.count, type: "model"});
        var s = seen[model], d = j;
        rv.links.push({source: s, target: d, value: 1});
        j++;
    }

    return rv;
}

function initVis() {
    var width = 1280,
        height = 800;

    var color = d3.scale.category20();

    var svg = d3.select("#chart").append("svg")
        .attr("width", width)
        .attr("height", height);

    function update() {
        d3.json(allMediaURL, function(json) {
            var nodes = computeNodesLinks(json.rows);

            var allSizes = [];
            for (var i = 0; i < nodes.nodes.length; i++) {
                allSizes.push(nodes.nodes[i].size);
            }

            var sizeScale = d3.scale.linear()
                .domain([d3.min(allSizes), d3.max(allSizes)])
                .range([5, 40]);

            var force = d3.layout.force()
                .charge(-400)
                .linkDistance(80)
                .size([width, height]);

            force
                .nodes(nodes.nodes)
                .links(nodes.links)
                .start();

            var link = svg.selectAll("line.link")
                .data(nodes.links)
                .enter().append("line")
                .attr("class", "link")
                .style("stroke-width", function(d) { return Math.sqrt(d.value); });

            svg.selectAll("line.link")
                .data(nodes.links)
                .exit().remove();

            var node = svg.selectAll("circle.node")
                .data(nodes.nodes)
                .enter().append("circle")
                .attr("class", function(d) { return "node " + d.type;})
                .attr("r", function(d) { return sizeScale(d.size); })
                .style("fill", function(d) {
                    return d.type == "model" ? color(d.name) : null;
                })
                .call(force.drag);

            node.append("title")
                .text(function(d) { return d.name + (d.type == 'root' ? '' : " (" + d.size + ")"); });

            svg.selectAll("circle.node")
                .data(nodes.nodes)
                .attr("class", function(d) { return "node " + d.type;})
                .attr("r", function(d) { return sizeScale(d.size); })
                .style("fill", function(d) {
                    return d.type == "model" ? color(d.name) : null;
                })
                .exit().remove();

            force.on("tick", function() {
                link.attr("x1", function(d) { return d.source.x; })
                    .attr("y1", function(d) { return d.source.y; })
                    .attr("x2", function(d) { return d.target.x; })
                    .attr("y2", function(d) { return d.target.y; });

                node.attr("cx", function(d) { return d.x; })
                    .attr("cy", function(d) { return d.y; });
            });
        });;
    }

    update();
}