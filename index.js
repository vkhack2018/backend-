const express       = require('express');
const bodyParser    = require('body-parser');
const pgp           = require('pg-promise')();
const md5           = require('md5');
const randomstring  = require("randomstring");
const parseDomain   = require("parse-domain");
const url           = require('url');
const request       = require('request');

const db = pgp({
    host: 'localhost',
    port: 5432,
    database: 'postgres',
    user: 'postgres',
    password: ''
});

const App = express();

const LINK_PROTO = "http";
const LINK_DOMAIN = "pony.pro";
const BASE_URL =  `${LINK_PROTO}://${LINK_DOMAIN}/`;

App.use(bodyParser.json());
App.use(bodyParser.urlencoded({
    extended: true
}));


let getVideoStatByUrl = async (_url) => {
    const API_TOKEN = 'AIzaSyA8mrDRRbES86ExgwSobAZNEW0teFF5Qs4';
    let videoId;
    try {
        videoId = url.parse(_url, true).query.v;
    }
    catch (e) {
        return Promise.reject(new Error(`could't parse video url: ${_url}`));
    }
    return new Promise((resolve, reject) => {
        request({
            uri: `https://www.googleapis.com/youtube/v3/videos?part=statistics&key=${API_TOKEN}&id=${videoId}`,
            method: 'GET',
        }, (error, response, body) => {
            if (!error && response.statusCode == 200) {
                try {
                    body = JSON.parse(body);
                }
                catch (e) {
                    reject(new Error(e.message))
                }
                resolve({
                    likes: body.items[0].statistics.likeCount,
                    dislikes: body.items[0].statistics.dislikeCount,
                    views: body.items[0].statistics.viewCount
                });
            }
            else {
                reject(new Error(`${error}`))
            }
        });
    });
}


App.post(`/blogger`, async (req, res) => {

    try {
        let blogger = req.body;

        let contentType = req.headers['content-type'];
        if (!contentType || contentType.indexOf('application/json')) {
            res.statusCode = 400;
            return res.send('Invalid content type');
        }

        if (blogger.name && blogger.channel_link) {

            blogger.channel_pic_url = blogger.channel_pic_url || '';
            blogger.subscribers = blogger.subscribers || '';

            let bloggerId = await db.one(
                `INSERT INTO bloggers(name, channel_link, channel_pic_url, subscribers) VALUES($1, $2, $3, $4) RETURNING id`,
                [blogger.name, blogger.channel_link, blogger.channel_pic_url, blogger.subscribers]
            );

            res.statusCode = 201;
            res.json({
                id: bloggerId.id,
                blogger
            });
        }
        else {
            res.statusCode = 400;
            res.send('Invalid params; Required: [name, channel_link]');
        }
    }
    catch (e) {
        console.log(`[/link]: ${e.name}\n${e.message}\n${e.stack}`);
        res.statusCode = 400;
        res.send(`Error: ${e.name}\n${e.message}\n${e.stack}`);
    }

});

App.post(`/content`, async (req, res) => {

    try {
        let content = req.body;

        let contentType = req.headers['content-type'];
        if (!contentType || contentType.indexOf('application/json')) {
            res.statusCode = 400;
            return res.send('Invalid content type');
        }

        if (content.type
            && content.url
            && content.blogger_id) {

            let views, likes, dislikes;

            if (content.likes === undefined
                || content.dislikes === undefined
                || content.views === undefined) {

                let stats;
                try {
                    stats = await getVideoStatByUrl(content.url);
                }
                catch (e) {
                    stats = {
                        views: 0,
                        likes: 0,
                        dislikes: 0
                    }
                }
                views = stats.views;
                likes = stats.likes;
                dislikes = stats.dislikes;
            }
            else {
                views = content.views;
                likes = content.likes;
                dislikes = content.dislikes;
            }

            content.name = content.name || '';
            content.picurl = content.picurl || '';

            let contentId = await db.one(
                `INSERT INTO content(type, url, blogger_id, views, likes, dislikes, name, picurl) VALUES($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
                [content.type, content.url, parseInt(content.blogger_id), parseInt(views), parseInt(likes), parseInt(dislikes), content.name, content.picurl]
            );

            res.statusCode = 201;
            res.json({
                id: contentId.id,
                content
            });
        }
        else {
            res.statusCode = 400;
            res.send('Invalid params; Required: [type, url, blogger_id]');
        }
    }
    catch (e) {
        console.log(`[/link]: ${e.name}\n${e.message}\n${e.stack}`);
        res.statusCode = 400;
        res.send(`Error: ${e.name}\n${e.message}\n${e.stack}`);
    }

});



App.post(`/links`, async (req, res) => {

    try {
        let link = req.body;

        let contentType = req.headers['content-type'];
        if (!contentType || contentType.indexOf('application/json')) {
            res.statusCode = 400;
            return res.send('Invalid content type');
        }

        if (link.description
            && link.content_id
            && link.blogger_id
            && link.long) {

            link.clicks = link.clicks || 0;

            let shortLink = (link.short !== undefined) ? link.short : BASE_URL + randomstring.generate({
                length: 16,
                charset: 'alphabetic'
            }); // md5(link.long).toString().substring(0, 16);


            let parsedUrl = parseDomain(link.long);
            let domain;
            let name;
            try {
                domain = `${parsedUrl.domain}.${parsedUrl.tld}`;
                name = parsedUrl.domain.toUpperCase();
            }
            catch (e) {
                throw new Error(`Domain parse error: ${e.message}\n\nsrc link: ${link.long}`);
            }

            let findSponsor = await db.query(`SELECT * FROM sponsor WHERE domain = $1`, [domain]);

            let sponsorId;
            if (findSponsor.length === 0) {
                sponsorId = (await db.one(
                    `INSERT INTO sponsor(name, domain) VALUES($1, $2) RETURNING id`,
                    [name, domain]
                )).id;
            }
            else {
                sponsorId = findSponsor[0].id;
            }


            let linkId = await db.one(
                `INSERT INTO links(description, content_id, blogger_id, sponsor_id, long, clicks, short, long_domain) VALUES($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
                [link.description, parseInt(link.content_id), parseInt(link.blogger_id), sponsorId, link.long, parseInt(link.clicks), shortLink, domain]
            );

            res.statusCode = 201;
            res.json({
                id: linkId.id,
                description: link.description,
                sponsorId: sponsorId,
                short: shortLink,
                link
            });
        }
        else {
            res.statusCode = 400;
            res.send('Invalid params; Required: [description, content_id, blogger_id, long]');
        }
    }
    catch (e) {
        console.log(`[/link]: ${e.name}\n${e.message}\n${e.stack}`);
        res.statusCode = 400;
        res.send(`Error: ${e.name}\n${e.message}\n${e.stack}`);
    }

});


App.post(`/links/batch`, async (req, res) => {

    try {
        let links = req.body;

        let contentType = req.headers['content-type'];
        if (!contentType || contentType.indexOf('application/json')) {
            res.statusCode = 400;
            return res.send('Invalid content type');
        }

        if (Array.isArray(links) !== true) {
            res.statusCode = 400;
            return res.send('Invalid content type. Require: array of objects');
        }

        let error = '';
        let response = [];
        for (let i in links) {

            let link = links[i];

            if (link.description
                && link.content_id
                && link.blogger_id
                && link.long) {

                link.clicks = link.clicks || 0;

                let shortLink = (link.short !== undefined) ? link.short : BASE_URL + randomstring.generate({
                    length: 16,
                    charset: 'alphabetic'
                }); // md5(link.long).toString().substring(0, 16);

                let parsedUrl = parseDomain(link.long);
                let domain;
                let name;
                try {
                    domain = `${parsedUrl.domain}.${parsedUrl.tld}`;
                    name = parsedUrl.domain.toUpperCase();
                }
                catch (e) {
                    throw new Error(`Domain parse error: ${e.message}\n\nsrc link: ${link.long}`);
                }


                let findSponsor = await db.query(`SELECT * FROM sponsor WHERE domain = $1`, [domain]);

                let sponsorId;
                if (findSponsor.length === 0) {
                    sponsorId = (await db.one(
                        `INSERT INTO sponsor(name, domain) VALUES($1, $2) RETURNING id`,
                        [name, domain]
                    )).id;
                }
                else {
                    sponsorId = findSponsor[0].id;
                }

                let linkId = await db.one(
                    `INSERT INTO links(description, content_id, blogger_id, sponsor_id, long, clicks, short, long_domain) VALUES($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
                    [link.description, parseInt(link.content_id), parseInt(link.blogger_id), sponsorId, link.long, parseInt(link.clicks), shortLink, domain]
                );

                response.push({
                    id: linkId.id,
                    description: link.description,
                    sponsorId: sponsorId,
                    short: shortLink,
                    link
                });
            }
            else {
               error += `Link number: ${i}; invalid params; Required: [description, content_id, blogger_id, long,]`;
            }
        }

        if (error.length === 0) {
            res.statusCode = 201;
            res.json(response);
        }
        else {
            res.statusCode = 400;
            res.send(error);
        }

    }
    catch (e) {
        console.log(`[/link/batch]: ${e.name}\n${e.message}\n${e.stack}`);
        res.statusCode = 400;
        res.send(`Error: ${e.name}\n${e.message}\n${e.stack}`);
    }

});



App.post(`/click`, async (req, res) => {

    try {
        let click = req.body;

        let contentType = req.headers['content-type'];
        if (!contentType || contentType.indexOf('application/json')) {
            res.statusCode = 400;
            return res.send('Invalid content type');
        }

        if (click.link_id
            && click.time) {

            let clickId = await db.one(
                `INSERT INTO clicks(link_id, time) VALUES($1, $2) RETURNING id`,
                [parseInt(click.link_id), parseInt(click.time)]
            );

            res.statusCode = 201;
            res.json({
                id: clickId.id,
                click
            });
        }
        else {
            res.statusCode = 400;
            res.send('Invalid params; Required: [link_id, time]');
        }
    }
    catch (e) {
        console.log(`[/click]: ${e.name}\n${e.message}\n${e.stack}`);
        res.statusCode = 400;
        res.send(`Error: ${e.name}\n${e.message}\n${e.stack}`);
    }

});



// REDIRECTOR
App.get(`/:linkId([a-zA-Z]{16})`, async (req, res) => {

    try {
        let linkId = req.params.linkId;
        if (/([a-zA-Z]){16}$/.test(linkId) === true) {
            let shortLink = BASE_URL + linkId;
            let links = await db.query(
                `SELECT * FROM links WHERE short = $1`,
                [shortLink]
            );

            console.log(links);
            if (links.length === 0) {
                res.statusCode = 404;
                res.send('Link not found');
            }
            else {
                await db.none(
                    `UPDATE links SET clicks = clicks + 1 WHERE short = $1`,
                    [shortLink]
                );
                res.redirect(links[0].long);
            }
        }
        else {
            res.statusCode = 400;
            res.send('Invalid format of link');
        }
    }
    catch (e) {
        console.log(`[redirector]: ${e.name}\n${e.message}\n${e.stack}`);
        res.statusCode = 400;
        res.send(`Error: ${e.name}\n${e.message}\n${e.stack}`);
    }
})
// REDIRECTOR




App.get(`/blogger/:bloggerId/content`, async (req, res) =>{

    try {
        let blodggerId = parseInt(req.params.bloggerId);

        let bloggerContentTopN = req.query.top || 20;
        bloggerContentTopN = parseInt(bloggerContentTopN);

        let blogger = await db.query(
            `SELECT * FROM bloggers WHERE id = $1`,
            [blodggerId]
        );

        if (blogger.length === 1) {

            let response = {
                id: blodggerId,
                name: blogger[0].name,
                channel_link: blogger[0].channel_link,
                channel_pic_url: blogger[0].channel_pic_url,
                subscribers: blogger[0].subscribers
            };

            let bloggerContent = await db.query(
              `SELECT content.*, SUM(links.clicks) AS total_clicks FROM content, links WHERE content.blogger_id = $1 AND content.id = links.content_id GROUP by content.id ORDER BY total_clicks DESC LIMIT ${bloggerContentTopN}`,
              [blodggerId]
            );

            let content = [];
            for (let i in bloggerContent) {
                let contentId = bloggerContent[i].id;

                let bloggerLinks = await db.query(
                    `SELECT * FROM links WHERE blogger_id = $1 AND content_id = $2`,
                    [blodggerId, contentId]
                );

                let links = [];
                let clicksSum = 0;
                for (let j in bloggerLinks) {
                    let linkId = bloggerLinks[j].id;
                    clicksSum += parseInt(bloggerLinks[j].clicks);
                    links.push({
                        id: linkId,
                        short: bloggerLinks[j].short,
                        long: bloggerLinks[j].long,
                        clicks: bloggerLinks[j].clicks,
                        description: bloggerLinks[j].description
                    });
                }

                content.push({
                    id: contentId,
                    type: bloggerContent[i].type,
                    url: bloggerContent[i].url,
                    picurl: bloggerContent[i].picurl,
                    name: bloggerContent[i].name,
                    views: bloggerContent[i].views,
                    likes: bloggerContent[i].likes,
                    dislikes: bloggerContent[i].dislikes,
                    links_count: links.length,
                    clicks_sum: clicksSum,
                    links: links
                });
            }

            response.content = content;

            res.statusCode = 200;
            res.json(response);
        }
        else {
            res.statusCode = 404;
            res.send(`Blogger not founded`);
        }
    }
    catch (e) {
        console.log(`[/blogger/:bloggerId/content]: ${e.name}\n${e.message}\n${e.stack}`);
        res.statusCode = 400;
        res.send(`Error: ${e.name}\n${e.message}\n${e.stack}`);
    }
});



App.get(`/content/:contentId`, async (req, res) =>{
    try {
        let contentId = parseInt(req.params.contentId);

        let content = await db.query(
            `SELECT * FROM content WHERE id = $1`,
            [contentId]
        );

        if (content.length === 1) {

            let response = {
                id: contentId,
                type: content[0].type,
                url: content[0].url,
                picurl: content[0].picurl,
                name: content[0].name,
                views: content[0].views,
                likes: content[0].likes,
                dislikes: content[0].dislikes
            };

            let contentLinks = await db.query(
                `SELECT * FROM links WHERE content_id = $1 ORDER BY clicks DESC`,
                [contentId]
            );

            let links = [];
            let clicksSum = 0;
            for (let i in contentLinks) {
                let linkId = contentLinks[i].id;
                clicksSum += parseInt(contentLinks[i].clicks);
                links.push({
                    id: linkId,
                    short: contentLinks[i].short,
                    long: contentLinks[i].long,
                    clicks: contentLinks[i].clicks,
                    description: contentLinks[i].description
                });
            }

            response.clicks_sum = clicksSum;
            response.links_count = links.length;
            response.links = links;

            res.statusCode = 200;
            res.json(response);
        }
        else {
            res.statusCode = 404;
            res.send(`Blogger not founded`);
        }
    }
    catch (e) {
        console.log(`[/content/:contentId]: ${e.name}\n${e.message}\n${e.stack}`);
        res.statusCode = 400;
        res.send(`Error: ${e.name}\n${e.message}\n${e.stack}`);
    }
});




App.get(`/sponsor/:sponsorId/top`, async (req, res) =>{
    try {
        let sponsorId = parseInt(req.params.sponsorId);

        let sponsor = await db.query(
            `SELECT * FROM sponsor WHERE id = $1`,
            [sponsorId]
        );

        if (sponsor.length === 1) {

            let sponsorDomain = sponsor[0].domain;
            let response = {
                id: sponsorId,
                name: sponsor[0].name,
                domain: sponsorDomain
            };

            let sponsorLinks = await db.query(
                `SELECT bloggers.*, SUM(links.clicks) AS clicks FROM links, bloggers WHERE links.long_domain = $1 AND links.blogger_id = bloggers.id  GROUP BY bloggers.id ORDER BY clicks DESC`,
                [sponsorDomain]
            );

            let bloggers = [];
            for (let i in sponsorLinks) {
                bloggers.push({
                    id: sponsorLinks[i].id,
                    name: sponsorLinks[i].name,
                    channel_link: sponsorLinks[i].channel_link,
                    channel_pic_url: sponsorLinks[i].channel_pic_url,
                    subscribers: parseInt(sponsorLinks[i].subscribers),
                    total_clicks: parseInt(sponsorLinks[i].clicks)
                });
            }
            response.bloggers  = bloggers;

            res.statusCode = 200;
            res.json(response);
        }
        else {
            res.statusCode = 404;
            res.send(`Sponsor id not founded`);
        }
    }
    catch (e) {
        console.log(`[/sponsor/:sponsorId/top]: ${e.name}\n${e.message}\n${e.stack}`);
        res.statusCode = 400;
        res.send(`Error: ${e.name}\n${e.message}\n${e.stack}`);
    }
});



//let _url = 'https://www.banggood.com/FrSky-ACCST-Taranis-Q-X7-Transmitter-2_4G-16CH-White-Black-International-Version-p-1196246.html?utm_source=Youtube&utm_medium=cussku&utm_campaign=436908_1205053&utm_content=10344&p=6Q0405436908201402OK';
//console.log(`${parseDomain(url.parse(_url).host).domain}.${parseDomain(url.parse(_url).host).tld}`);

App.listen(8080, () => {
    console.log('Server running...');
});


