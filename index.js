const dmf_ms = require('dmf_ms');
const config = require('./config');
const { Client, Pool } = require('pg');
const bluebird = require('bluebird');
bluebird.config({longStackTraces: true});

let app = new dmf_ms({
    name: config.MQservice.name,
    microservices: config.MQservice.microservices,
    url: config.MQservice.rabbitUrl
});

app.createEnvironment = async ({userid}, {userType}) => {
    let client = new Client({
        Promise: bluebird,
        user: config.postgres.user,
        password: config.postgres.password,
        host: config.postgres.host,
        port: config.postgres.port,
        database: config.postgres.database
    });
    await client.connect();

    let user = {};
    if (userid) {
        let rows = (await client.query(`select * from teachers where id = $1`, [userid])).rows;
        if (rows.length) user = {type: "teacher", ...rows[0]};
        else {
            rows = (await client.query(`select * from students where id = $1`, [userid])).rows;
            if (rows.length) user = {type: "student", ...rows[0]};
            else user = {id: userid};
        }
    }

    let incorrectUserType = false;
    if (userType == "all" & user.type === undefined) incorrectUserType = true;
    if (userType == "teacher" & user.type != "teacher") incorrectUserType = true;
    if (userType == "student" & user.type != "student") incorrectUserType = true;
    if (incorrectUserType) {
        await client.end();
        app.exit({message: "Нет доступа"});
    }

    return {
        user: user,
        db: {
            query: async function() {
                return (await client.query(...arguments)).rows
            },
            transaction: async function(f) {
                try {
                    await client.query('BEGIN');
                    await f();
                    await client.query('COMMIT');
                }
                catch(err) {
                    await client.query('ROLLBACK');
                    throw err;
                }
            },
            disconnect: async function() {
                await client.end()
            }
        }
    }
}

app.destroyEnvironment = async ({db}) => {
    await db.disconnect()
}

app.action({
    name: "ping",
    func: function() {
        return "pong"
    }
})


//создание пользователя

app.action({
    name: "create_user",
    options: {userType: "teacher"},
    func: async function({name, is_student}) {
        let user = this.user
        let db = this.db
        let ask = this.ask

        if (is_student || user.is_admin) {
            let obj_encrypt = await ask({
                service: config.MQservices.auth,
                action: 'encrypt',
                params: {data: {teacher_id: user.id, name, is_student}}
            })

            return ask({
                service: config.MQservices.auth,
                action: 'geturl',
                params: {
                    appl: config.apps.linkWithAuth,
                    params: {
                        service: config.MQservice.name,
                        func: 'register_user',
                        params: obj_encrypt,
                        appllinks: config.apps.links,
                    }
                }
            })
        }
        else app.exit({message: "Нет доступа"})
    }
})

app.action({
    name: "register_user",
    func: async function(obj_encrypt) {
        let user = this.user
        let db = this.db
        let ask = this.ask

        let {teacher_id, name, is_student} = await ask({
            service: config.MQservices.auth,
            action: 'decrypt',
            params: {data: obj_encrypt}
        })

        await db.transaction(async () => {
            if ((await db.query(`select * from students where id = $1`, [user.id])).length) {
                return
            }
            if ((await db.query(`select * from teachers where id = $1`, [user.id])).length) {
                return
            }

            if (is_student) {
                await db.query(`insert into students (id, teacher_id, name) values ($1, $2, $3) on conflict(id) do nothing`, [user.id, teacher_id, name])
            }
            else {
                await db.query(`insert into teachers (id, name) values ($1, $2) on conflict(id) do nothing`, [user.id, name])
            }
        })

        let url = await ask({
            service: config.MQservices.auth,
            action: 'getapplurl',
            params: {name: config.apps.tareas}
        })

        return url
    }
})


//работа с файлами

app.action({
    name: "create_file",
    options: {userType: "all"},
    func: async function({temp_id}) {
        if(typeof this.file.key !== 'string') {
            return {checkAccessUploadFile: true}
        }
        else {
            await this.db.query(`insert into temp_files (temp_id, id) values ($1, $2)`, [temp_id, this.file.key])
            return {name: this.file.originalname}
        }
    }
})

async function get_file_ids(db, files) {
    let res = {}
    for (let [key, temp_id] of Object.entries(files)) {
        res[key] = (await db.query(`delete from temp_files where temp_id = $1 returning id`, [temp_id]))[0].id;
    }
    return res;
}

async function get_file_urls(ask, files) {
    let links = await Promise.all(
        Object.entries(files).map(async ([key, id]) => {
            link = await ask({
                service: config.MQservices.s3,
                action: "getUrl",
                params: {key: id}
            })
            return [key, link]
        })
    )
    let res = {}
    for ([key, link] of links) res[key] = link
    return res;
}


//инфа о пользователях

app.action({
    name: "get_user_info",
    options: {userType: "all"},
    func: async function() {
        let info = {
            type: this.user.type,
            name: this.user.name
        }
        if (this.user.type == "teacher") info.is_admin = this.user.is_admin
        return info
    }
})

app.action({
    name: "get_students",
    options: {userType: "teacher"},
    func: async function() {
        let user = this.user;
        let db = this.db;
        return db.query(`select * from students where teacher_id = $1`, [user.id])
    }
})


//задания

app.action({
    name: "get_task_types",
    options: {userType: "all"},
    func: async function() {
        return this.db.query(`select * from task_types`)
    }
})

app.action({
    name: "get_teacher_tasks",
    options: {userType: "all"},
    func: async function() {
        if (this.user.type == "teacher") {
            return this.db.query(
                `select
                     tasks.id,
                     tasks.type_id,
                     tasks.name,
                     coalesce(solved.count, 0) as solved_count
                from tasks
                left join (
                    select task_id, count(*)
                    from solved_tasks
                    group by task_id
                ) as solved
                on tasks.id = solved.task_id
                where teacher_id = $1`,
                [this.user.id]
            );
        }
        else {
            return this.db.query(`select id, type_id, name from tasks where teacher_id = $1`, [this.user.teacher_id])
        }
    }
})

app.action({
    name: "get_teacher_task",
    options: {userType: "all"},
    func: async function({id}) {
        let user = this.user;
        let db = this.db;
        let teacher_id = (user.type == "teacher") ? user.id : user.teacher_id;

        let rows = await db.query(`select name, files, other_info from tasks where id = $1 and teacher_id = $2`, [id, teacher_id]);
        if (!rows.length) app.exit({message: "Нет доступа"});

        let task = rows[0];
        task.files = await get_file_urls(this.ask, task.files);
        return task;
    }
})

app.action({
    name: "get_student_tasks",
    options: {userType: "all"},
    func: async function({id}) {
        let user = this.user;
        let db = this.db;

        if (user.type == "student") id = user.id;
        else {
            let rows = await db.query(`select * from students where id = $1 and teacher_id = $2`, [id, user.id])
            if (!rows.length) app.exit({message: "Нет доступа"});
        }

        return db.query(
            `select
                tasks.id,
                tasks.type_id,
                tasks.name
            from solved_tasks inner join tasks on solved_tasks.task_id = tasks.id
            where solved_tasks.student_id = $1`,
            [id]
        )
    }
})

app.action({
    name: "get_student_task",
    options: {userType: "all"},
    func: async function({task_id, student_id}) {
        let user = this.user;
        let db = this.db;

        if (user.type == "student") student_id = user.id
        else {
            let rows = await db.query(`select * from students where id = $1 and teacher_id = $2`, [student_id, user.id]);
            if (!rows.length) app.exit({message: "Нет доступа"});
        }

        let rows = await db.query(`select files, other_info from solved_tasks where task_id = $1 and student_id = $2`, [task_id, student_id])
        if (!rows.length) app.exit({message: "Задание не найдено"});

        let task = rows[0];
        task.files = await get_file_urls(this.ask, task.files);
        return task;
    }
})

app.action({
    name: "create_task",
    options: {userType: "teacher"},
    func: async function({type_id, name, files={}, other_info={}}) {
        user = this.user;
        db = this.db;

        await db.transaction(async () => {
            files = await get_file_ids(db, files);
            await db.query(`insert into tasks (type_id, teacher_id, name, files, other_info) values ($1, $2, $3, $4, $5)`, [type_id, user.id, name, files, other_info])
        })
    }
})

app.action({
    name: "update_task",
    options: {userType: "teacher"},
    func: async function({id, name, files={}, other_info=null}) {
        user = this.user;
        db = this.db;

        await db.transaction(async () => {
            let rows = await db.query(`select name, files, other_info from tasks where id = $1 and teacher_id = $2`, [id, user.id])
            if (!rows.length) app.exit({message: "Нет доступа"})

            let task = rows[0];
            files = await get_file_ids(db, files);
            name = name ? name : task.name;
            files = {...task.files, ...files};
            other_info = {...task.other_info, ...other_info};

            await db.query(`update tasks set name = $1, files = $2, other_info = $3 where id = $4`, [name, files, other_info, id])
        })
    }
})

app.action({
    name: "delete_task",
    options: {userType: "teacher"},
    func: async function({id}) {
        await this.db.query(`delete from tasks where id = $1 and teacher_id = $2`, [id, this.user.id]);
    }
})

app.action({
    name: "solve_task",
    options: {userType: "student"},
    func: async function({task_id, files={}, other_info={}}) {
        user = this.user;
        db = this.db;

        await db.transaction(async () => {
            let rows = await db.query(`select * from tasks where id = $1 and teacher_id = $2`, [task_id, user.teacher_id])
            if (!rows.length) app.exit({message: "Нет доступа"})

            files = await get_file_ids(db, files);
            await db.query(
                `insert into solved_tasks (task_id, student_id, files, other_info)
                values ($1, $2, $3, $4)
                on conflict (task_id, student_id)
                do update set files = EXCLUDED.files, other_info = EXCLUDED.other_info`,
                [task_id, user.id, files, other_info]
            )
        })
    }
})