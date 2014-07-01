"use strict"

$(document).ready(function() {

    var language = {
        "sProcessing": "Подождите...",
        "sLengthMenu": "Показать _MENU_ записей",
        "sZeroRecords": "Записи отсутствуют.",
        "sInfo": "Записи с _START_ до _END_ из _TOTAL_ записей",
        "sInfoEmpty": "Записи с 0 до 0 из 0 записей",
        "sInfoFiltered": "(отфильтровано из _MAX_ записей)",
        "sInfoPostFix": "",
        "sSearch": "Поиск:",
        "sUrl": "",
        "oPaginate": {
            "sFirst": "Первая",
            "sPrevious": "Предыдущая",
            "sNext": "Следующая",
            "sLast": "Последняя"
        },
        "oAria": {
            "sSortAscending": ": активировать для сортировки столбца по возрастанию",
            "sSortDescending": ": активировать для сортировки столбцов по убыванию"
        }
    };

    function init_month_table(api_string) {
        $('#users-month').dataTable({
            ajax: api_string,
            language: language,
            order: [
                [0, "asc"]
            ],
            columnDefs: [{
                    "targets": [2, 3],
                    "render": function(data, type, full, meta) {
                        if (type != 'display') return data;
                        var k = ['', 'K', 'M', 'G'],
                            i = 0;
                        while (data > 1024 && i < 4) {
                            data /= 1024;
                            i++;
                        }
                        return Math.round(data) + k[i];
                    }
                }, {
                    "targets": 0,
                    "render": function(data, type) {
                        if (type != 'display') return data;
                        var link = data.split('-').join('/');
                        return "<a href='/users/" + link + "' class='datalink'>" + data + "</a>";
                    }
                }, {
                    "targets": 1,
                    "render": function(data, type, full) {
                        if (type != 'display') return data;
                        return full[2] + data > 0 ? Math.round(100 * data / (data + full[2])) + "%" : "-";
                    }
                }

            ]
        });
    }

    function init_day_table(api_string, year, month, day) {
        $('#users-day').dataTable({
            ajax: api_string,
            language: language,
            order: [
                [2, "desc"]
            ],
            columnDefs: [{
                "targets": [2, 3],
                "render": function(data, type, full, meta) {
                    if (type != 'display') return data;
                    var k = ['', 'K', 'M', 'G'],
                        i = 0;
                    while (data > 1024 && i < 4) {
                        data /= 1024;
                        i++;
                    }
                    return Math.round(data) + k[i];
                }
            }, {
                "targets": 1,
                "render": function(data, type, full) {
                    if (type != 'display') return data;
                    return full[2] + data > 0 ? Math.round(100 * data / (full[2] + data)) + "%" : "-";
                }
            }, {
                "targets": 0,
                "render": function(data, type) {
                    if (type != 'display') return data;
                    var link = [data, year, month, day].join('/');
                    return "<a href='/user/" + link + "' class='datalink'>" + data + "</a>";
                }
            }]
        });
    }

    function init_user_table(api_string) {
        $('#user-day').dataTable({
            ajax: api_string,
            language: language,
            order: [
                [2, "desc"]
            ],
            columnDefs: [{
                "targets": [2, 3],
                "render": function(data, type, full, meta) {
                    if (type != 'display') return data;
                    var k = ['', 'K', 'M', 'G'],
                        i = 0;
                    while (data > 1024 && i < 4) {
                        data /= 1024;
                        i++;
                    }
                    return Math.round(data) + k[i];
                }
            }, {
                "targets": 1,
                "render": function(data, type, full) {
                    if (type != 'display') return data;
                    return (full[2] + data) > 0 ? Math.round(100 * data / (full[2] + data)) + "%" : "-";
                }
            }]
        });
    }


    var api_string = '/api/';
    var path = location.pathname.split('/');
    path.shift();
    if (path[path.length - 1] == '') path.pop();

    /* /users */
    if (path.length == 1) {
        var d = new Date();
        api_string += d.getFullYear() + '/' + (d.getMonth() + 1);
        init_month_table(api_string);
        $('#panel-month').removeClass('hide');
    } else if (path.length == 3) {
        api_string += path[1] + '/' + path[2];
        init_month_table(api_string);
        $('#panel-month').removeClass('hide');
    } else if (path.length == 4) {
        api_string += path[1] + '/' + path[2] + '/' + path[3];
        init_day_table(api_string, path[1], path[2], path[3]);
        $('#panel-day').removeClass('hide');
    } else if (path.length == 5) {
        api_string += 'user/' + path[1] + '/' + path[2] + '/' + path[3] + '/' + path[4];
        init_user_table(api_string);
        $('#panel-user-day').removeClass('hide');
    }

    $('#side-menu > li:nth-child(3)').addClass('active')

});
